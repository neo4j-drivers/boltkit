#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2016 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
***************************
How To Build a Neo4j Driver
***************************

Welcome to the all-in-one-file guide to how to build a Neo4j driver. This file is intended to be
read from top to bottom, giving an incremental description of the pieces required to construct a
Neo4j database driver from scratch in any language. Python has been chosen as the language to
illustrate this process due to the inherent readability of Python source code as well as
Python's comprehensive standard library and the fact that minimal boilerplate is required.

Note that while this driver is complete, it is neither supported, nor intended for use in a
production environment. You can of course decide to do ignore this and do so anyway but if you do,
you're on your own!

So, let's get started.

Neo4j provides a binary protocol, called Bolt, which is what we are actually targeting here. A
Neo4j Bolt driver can be thought of as composed of three layers...

1. Data serialisation. For this, we use a custom serialisation format called PackStream. While the
   design of this format is inspired heavily by MessagePack, it is not compatible with it.
   PackStream provides a type system that is fully compatible with the Cypher type system used by
   Neo4j and also takes extension data types in a different direction to MessagePack. More on
   PackStream shortly.
2. Bolt connections. At its heart, the Bolt protocol provides a stateful request-response
   mechanism. Each request consists of a textual statement plus a map or dictionary of
   parameters; each response is comprised of a stream of content plus some summary metadata.
   Message pipelining comes for free: a Bolt server will queue requests and respond to them in
   the same order in which they were received. A Bolt client therefore has a degree of
   flexibility in how and when it sends requests and how and when it gathers the responses.
3. The Session API. Compliant drivers adhere to a standardised API design that sits atop the
   messaging layer, commonly known as the Session API. This provides a consistent vocabulary and
   pattern of usage for application developers, regardless of language. Though this is of course a
   minimum: any driver author should feel free to innovate around this and provide any amount of
   language-idiomatic extras that are appropriate or desirable.

"""

# You'll need to make sure you have the following items handy...
from logging import getLogger
from socket import create_connection
from struct import pack as raw_pack, unpack_from as raw_unpack


# CHAPTER 1: PACKSTREAM SERIALISATION
# ===================================

# First stop: PackStream. Python provides a module called `struct` for coercing data to and from
# binary representations of that data. The format codes below are the ones that PackStream cares
# about and each has been given a handy name to make the code that uses it easier to follow. The
# second character in each of these codes (the letter) represents the actual data type, the first
# character (the '>' symbol) denotes that all our representations should be big-endian. This
# means that the most significant part of the value is written to the network or memory space first
# and the least significant part is written last. PackStream thinks entirely in big ends.
#
INT_8 = ">b"        # signed 8-bit integer (two's complement)
INT_16 = ">h"       # signed 16-bit integer (two's complement)
INT_32 = ">i"       # signed 32-bit integer (two's complement)
INT_64 = ">q"       # signed 64-bit integer (two's complement)
UINT_8 = ">B"       # unsigned 8-bit integer
UINT_16 = ">H"      # unsigned 16-bit integer
UINT_32 = ">I"      # unsigned 32-bit integer
FLOAT_64 = ">d"     # IEEE double-precision floating-point format


# The PackStream type system supports a set of commonly-used data types (plus null) as well as
# extension types called "structures" that can be used to represent composite values. The full list
# of types is:
#
#   - Null (absence of value)
#   - Boolean (true or false)
#   - Integer (signed 64-bit integer)
#   - Float (64-bit floating point number)
#   - String (UTF-8 encoded text data)
#   - List (ordered collection of values)
#   - Map (keyed collection of values)
#   - Structure (composite set of values with a type tag)
#
# Neither unsigned integers nor byte arrays are supported but may be added in a future version of
# the format. Note that 32-bit floating point numbers are also not supported. This is a deliberate
# decision and these won't be added in any future version.


# Oh, by the way, we use hexadecimal a lot here. If you're not familiar with that, you might want
# to take a short break and hop over to Wikipedia to read up about it before going much further...


class Structure:

    def __init__(self, tag, *fields):
        self.tag = tag
        self.fields = fields

    def __eq__(self, other):
        return self.tag == other.tag and self.fields == other.fields

    def __ne__(self, other):
        return not self.__eq__(other)


def packed(*values):
    """ This function provides PackStream values-to-bytes functionality, a process known as
    "packing". The tag of the method permits any number of values to be provided as
    positional arguments. Each will be serialised in order into the output byte stream.

    Markers
    -------
    Every serialised value begins with a marker byte. The marker contains information on data type
    as well as direct or indirect size information for those types that require it. How that size
    information is encoded varies by marker type.

    Some values, such as boolean true, can be encoded within a single marker byte. Many small
    integers (specifically between -16 and +127 inclusive) are also encoded within a single byte.

    A number of marker bytes are reserved for future expansion of the format itself. These bytes
    should not be used, and encountering them in an incoming stream should treated as an error.

    Sized Values
    ------------
    Some value types require variable length representations and, as such, have their size
    explicitly encoded. These values generally begin with a single marker byte, followed by a size,
    followed by the data content itself. Here, the marker denotes both type and scale and therefore
    determines the number of bytes used to represent the size of the data. The size itself is
    either an 8-bit, 16-bit or 32-bit unsigned integer. Sizes longer than this are not yet
    supported.

    The diagram below illustrates the general layout for a sized value, here with a 16-bit size:

      Marker Size          Content
        <>  <--->  <--------------------->
        XX  XX XX  XX XX XX XX .. .. .. XX


    Args:
        values: Series of values to pack.

    Returns:
        Byte representation of values.
    """

    # First, let's define somewhere to collect the individual byte pieces and grab a couple of
    # handles to commonly-used methods.
    #
    data = []
    append = data.append
    extend = data.extend

    # Next we'll iterate through the values in turn and add the output to our collection of byte
    # pieces.
    #
    for value in values:

        # Null is always encoded using the single marker byte C0.
        #
        if value is None:
            append(b"\xC0")

        # Boolean values are encoded within a single marker byte, using C3 to denote true and C2
        # to denote false.
        #
        elif value is True:
            append(b"\xC3")
        elif value is False:
            append(b"\xC2")

        # Integers
        # --------
        # Integer values occupy either 1, 2, 3, 5 or 9 bytes depending on magnitude. Several
        # markers are designated specifically as TINY_INT values and can therefore be used to pass
        # a small number in a single byte. These markers can be identified by a zero high-order bit
        # (for positive values) or by a high-order nibble containing only ones (for negative
        # values). The available encodings are illustrated below and each shows a valid
        # representation for the decimal value 42:
        #
        #     2A                          -- TINY_INT
        #     C8:2A                       -- INT_8
        #     C9:00:2A                    -- INT_16
        #     CA:00:00:00:2A              -- INT_32
        #     CB:00:00:00:00:00:00:00:2A  -- INT_64
        #
        # Note that while encoding small numbers in wider formats is supported, it is generally
        # recommended to use the most compact representation possible. The following table shows
        # the optimal representation for every possible integer:
        #
        #    Range Minimum             |  Range Maximum             | Representation
        #  ============================|============================|================
        #   -9 223 372 036 854 775 808 |             -2 147 483 649 | INT_64
        #               -2 147 483 648 |                    -32 769 | INT_32
        #                      -32 768 |                       -129 | INT_16
        #                         -128 |                        -17 | INT_8
        #                          -16 |                       +127 | TINY_INT
        #                         +128 |                    +32 767 | INT_16
        #                      +32 768 |             +2 147 483 647 | INT_32
        #               +2 147 483 648 | +9 223 372 036 854 775 807 | INT_64
        #
        elif isinstance(value, int):
            if -0x10 <= value < 0x80:
                append(raw_pack(INT_8, value))                          # TINY_INT
            elif -0x80 <= value < 0x80:
                append(b"\xC8")
                append(raw_pack(INT_8, value))                          # INT_8
            elif -0x8000 <= value < 0x8000:
                append(b"\xC9")
                append(raw_pack(INT_16, value))                         # INT_16
            elif -0x80000000 <= value < 0x80000000:
                append(b"\xCA")
                append(raw_pack(INT_32, value))                         # INT_32
            elif -0x8000000000000000 <= value < 0x8000000000000000:
                append(b"\xCB")
                append(raw_pack(INT_64, value))                         # INT_64
            else:
                raise ValueError("Integer value out of packable range")

        # Floating Point Numbers
        # ----------------------
        # These are double-precision floating-point values, generally used for representing
        # fractions and decimals. Floats are encoded as a single C1 marker byte followed by 8
        # bytes which are formatted according to the IEEE 754 floating-point "double format" bit
        # layout.
        #
        # - Bit 63 (the bit that is selected by the mask `0x8000000000000000`) represents
        #   the sign of the number.
        # - Bits 62-52 (the bits that are selected by the mask `0x7ff0000000000000`)
        #   represent the exponent.
        # - Bits 51-0 (the bits that are selected by the mask `0x000fffffffffffff`)
        #   represent the significand (sometimes called the mantissa) of the number.
        #
        #     C1 3F F1 99 99 99 99 99 9A  -- Float(+1.1)
        #     C1 BF F1 99 99 99 99 99 9A  -- Float(-1.1)
        #
        elif isinstance(value, float):
            append(b"\xC1")
            append(raw_pack(FLOAT_64, value))

        # Strings
        # -------
        # Text data is represented as UTF-8 encoded bytes. Note that the sizes used in string
        # representations are the byte counts of the UTF-8 encoded data, not the character count
        # of the original text.
        #
        #   Marker | Size                                        | Maximum size
        #  ========|=============================================|=====================
        #   80..8F | contained within low-order nibble of marker | 15 bytes
        #   D0     | 8-bit big-endian unsigned integer           | 255 bytes
        #   D1     | 16-bit big-endian unsigned integer          | 65 535 bytes
        #   D2     | 32-bit big-endian unsigned integer          | 4 294 967 295 bytes
        #
        # For encoded text containing fewer than 16 bytes, including empty strings, the marker
        # byte should contain the high-order nibble '8' (binary 1000) followed by a low-order
        # nibble containing the size. The encoded data then immediately follows the marker.
        #
        # For encoded text containing 16 bytes or more, the marker D0, D1 or D2 should be used,
        # depending on scale. This marker is followed by the size and the UTF-8 encoded data.
        # Examples follow below:
        #
        #     "" -> 80
        #
        #     "A" -> 81:41
        #
        #     "ABCDEFGHIJKLMNOPQRSTUVWXYZ" -> D0:1A:41:42:43:44:45:46:47:48:49:4A:4B:4C
        #                                     4D:4E:4F:50:51:52:53:54:55:56:57:58:59:5A
        #
        #     "Größenmaßstäbe" -> D0:12:47:72:C3:B6:C3:9F:65:6E:6D:61:C3:9F:73:74:C3:A4:62:65
        #
        elif isinstance(value, str):
            utf_8 = value.encode("UTF-8")
            size = len(utf_8)
            if size < 0x10:
                append(raw_pack(UINT_8, 0x80 + size))
            elif size < 0x100:
                append(b"\xD0")
                append(raw_pack(UINT_8, size))
            elif size < 0x10000:
                append(b"\xD1")
                append(raw_pack(UINT_16, size))
            elif size < 0x100000000:
                append(b"\xD2")
                append(raw_pack(UINT_32, size))
            else:
                raise ValueError("String too long to pack")
            append(utf_8)

        # Lists
        # -----
        # Lists are heterogeneous sequences of values and therefore permit a mixture of types
        # within the same list. The size of a list denotes the number of items within that list,
        # rather than the total packed byte size. The markers used to denote a list are described
        # in the table below:
        #
        #   Marker | Size                                         | Maximum size
        #  ========|==============================================|=====================
        #   90..9F | contained within low-order nibble of marker  | 15 bytes
        #   D4     | 8-bit big-endian unsigned integer            | 255 items
        #   D5     | 16-bit big-endian unsigned integer           | 65 535 items
        #   D6     | 32-bit big-endian unsigned integer           | 4 294 967 295 items
        #
        # For lists containing fewer than 16 items, including empty lists, the marker byte should
        # contain the high-order nibble '9' (binary 1001) followed by a low-order nibble containing
        # the size. The items within the list are then serialised in order immediately after the
        # marker.
        #
        # For lists containing 16 items or more, the marker D4, D5 or D6 should be used, depending
        # on scale. This marker is followed by the size and list items, serialized in order.
        # Examples follow below:
        #
        #     [] -> 90
        #
        #     [1, 2, 3] -> 93:01:02:03
        #
        #     [1, 2.0, "three"] -> 93:01:C1:40:00:00:00:00:00:00:00:85:74:68:72:65:65
        #
        #     [1, 2, 3, ... 40] -> D4:28:01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F:10
        #                          10:11:12:13:14:15:16:17:18:19:1A:1B:1C:1D:1E:1F:20:22
        #                          23:24:25:26:27:28
        #
        elif isinstance(value, list):
            size = len(value)
            if size < 0x10:
                append(raw_pack(UINT_8, 0x90 + size))
            elif size < 0x100:
                append(b"\xD4")
                append(raw_pack(UINT_8, size))
            elif size < 0x10000:
                append(b"\xD5")
                append(raw_pack(UINT_16, size))
            elif size < 0x100000000:
                append(b"\xD6")
                append(raw_pack(UINT_32, size))
            else:
                raise ValueError("List too long to pack")
            extend(map(packed, value))

        # Maps
        # ----
        # Maps are sets of key-value pairs that permit a mixture of types within the same map. The
        # size of a map denotes the number of pairs within that map, not the total packed byte
        # size. The markers used to denote a map are described in the table below:
        #
        #   Marker | Size                                         | Maximum size
        #  ========|==============================================|=======================
        #   A0..AF | contained within low-order nibble of marker  | 15 entries
        #   D8     | 8-bit big-endian unsigned integer            | 255 entries
        #   D9     | 16-bit big-endian unsigned integer           | 65 535 entries
        #   DA     | 32-bit big-endian unsigned integer           | 4 294 967 295 entries
        #
        # For maps containing fewer than 16 key-value pairs, including empty maps, the marker byte
        # should contain the high-order nibble 'A' (binary 1010) followed by a low-order nibble
        # containing the size. The entries within the map are then serialised in [key, value,
        # key, value] order immediately after the marker. Keys are generally text values.
        #
        # For maps containing 16 pairs or more, the marker D8, D9 or DA should be used, depending
        # on scale. This marker is followed by the size and map entries. Examples follow below:
        #
        #     {} -> A0
        #
        #     {"one": "eins"} -> A1:83:6F:6E:65:84:65:69:6E:73
        #
        #     {"A": 1, "B": 2 ... "Z": 26} -> D8:1A:81:45:05:81:57:17:81:42:02:81:4A:0A:81:41:01
        #                                     81:53:13:81:4B:0B:81:49:09:81:4E:0E:81:55:15:81:4D
        #                                     0D:81:4C:0C:81:5A:1A:81:54:14:81:56:16:81:43:03:81
        #                                     59:19:81:44:04:81:47:07:81:46:06:81:50:10:81:58:18
        #                                     81:51:11:81:4F:0F:81:48:08:81:52:12
        #
        # The order in which map entries are encoded is not important; maps are, by definition,
        # unordered.
        #
        elif isinstance(value, dict):
            size = len(value)
            if size < 0x10:
                append(raw_pack(UINT_8, 0xA0 + size))
            elif size < 0x100:
                append(b"\xD8")
                append(raw_pack(UINT_8, size))
            elif size < 0x10000:
                append(b"\xD9")
                append(raw_pack(UINT_16, size))
            elif size < 0x100000000:
                append(b"\xDA")
                append(raw_pack(UINT_32, size))
            else:
                raise ValueError("Dictionary too long to pack")
            extend(packed(k, v) for k, v in value.items())

        # Structures
        # ----------
        # Structures represent composite values and consist, beyond the marker, of a single byte
        # tag followed by a sequence of fields, each an individual value. The size of a
        # structure is measured as the number of fields and not the total byte size. This count
        # does not include the tag. The markers used to denote a structure are described in
        # the table below:
        #
        #   Marker | Size                                        | Maximum size
        #  ========|=============================================|=======================
        #   B0..BF | contained within low-order nibble of marker | 15 fields
        #   DC     | 8-bit big-endian unsigned integer           | 255 fields
        #   DD     | 16-bit big-endian unsigned integer          | 65 535 fields
        #
        # The tag byte is used to identify the type or class of the structure. Tag
        # bytes may hold any value between 0 and +127. Bytes with the high bit set are reserved
        # for future expansion. For structures containing fewer than 16 fields, the marker byte
        # should contain the high-order nibble 'B' (binary 1011) followed by a low-order nibble
        # containing the size. The marker is immediately followed by the tag byte and the
        # field values.
        #
        # For structures containing 16 fields or more, the marker DC or DD should be used,
        # depending on scale. This marker is followed by the size, the tag byte and the
        # fields, serialised in order. Examples follow below:
        #
        #     B3 01 01 02 03  -- Struct(sig=0x01, fields=[1,2,3])
        #     DC 10 7F 01  02 03 04 05  06 07 08 09  00 01 02 03
        #     04 05 06  -- Struct(sig=0x7F, fields=[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6]
        #
        elif isinstance(value, Structure):
            size = len(value.fields)
            if size < 0x10:
                append(raw_pack(UINT_8, 0xB0 + size))
            elif size < 0x100:
                append(b"\xDC")
                append(raw_pack(UINT_8, size))
            elif size < 0x10000:
                append(b"\xDD")
                append(raw_pack(UINT_16, size))
            else:
                raise ValueError("Structure too big to pack")
            append(raw_pack(UINT_8, value.tag))
            extend(map(packed, value.fields))

        # For anything else, we'll just raise an error as we don't know how to encode it.
        #
        else:
            raise ValueError("Cannot pack value %r" % (value,))

    # Finally, we can glue all the individual pieces together and return the full byte
    # representation of the original values.
    #
    return b"".join(data)


class Packed:
    """ The Packed class provides a framework for "unpacking" packed data. Given a string of byte
    data and an initial offset, values can be extracted via the unpack method.
    """

    def __init__(self, data, offset=0):
        self.data = data
        self.offset = offset

    def raw_unpack(self, type_code):
        value, = raw_unpack(type_code, self.data, self.offset)
        self.offset += {
            INT_8: 1, INT_16: 2, INT_32: 4, INT_64: 8,
            UINT_8: 1, UINT_16: 2, UINT_32: 4, FLOAT_64: 8,
        }[type_code]
        return value

    def unpack_string(self, size):
        end = self.offset + size
        value = self.data[self.offset:end].decode("UTF-8")
        self.offset = end
        return value

    def unpack(self, count=1):
        for _ in range(count):
            marker_byte = self.raw_unpack(UINT_8)
            if marker_byte == 0xC0:
                yield None
            elif marker_byte == 0xC3:
                yield True
            elif marker_byte == 0xC2:
                yield False
            elif marker_byte < 0x80:
                yield marker_byte
            elif marker_byte >= 0xF0:
                yield marker_byte - 0x100
            elif marker_byte == 0xC8:
                yield self.raw_unpack(INT_8)
            elif marker_byte == 0xC9:
                yield self.raw_unpack(INT_16)
            elif marker_byte == 0xCA:
                yield self.raw_unpack(INT_32)
            elif marker_byte == 0xCB:
                yield self.raw_unpack(INT_64)
            elif marker_byte == 0xC1:
                yield self.raw_unpack(FLOAT_64)
            elif 0x80 <= marker_byte < 0x90:
                yield self.unpack_string(marker_byte & 0x0F)
            elif marker_byte == 0xD0:
                yield self.unpack_string(self.raw_unpack(UINT_8))
            elif marker_byte == 0xD1:
                yield self.unpack_string(self.raw_unpack(UINT_16))
            elif marker_byte == 0xD2:
                yield self.unpack_string(self.raw_unpack(UINT_32))
            elif 0x90 <= marker_byte < 0xA0:
                yield list(self.unpack(marker_byte & 0x0F))
            elif marker_byte == 0xD4:
                yield list(self.unpack(self.raw_unpack(UINT_8)))
            elif marker_byte == 0xD5:
                yield list(self.unpack(self.raw_unpack(UINT_16)))
            elif marker_byte == 0xD6:
                yield list(self.unpack(self.raw_unpack(UINT_32)))
            elif 0xA0 <= marker_byte < 0xB0:
                yield dict(tuple(self.unpack(2)) for _ in range(marker_byte & 0x0F))
            elif marker_byte == 0xD8:
                yield dict(tuple(self.unpack(2)) for _ in range(self.raw_unpack(UINT_8)))
            elif marker_byte == 0xD9:
                yield dict(tuple(self.unpack(2)) for _ in range(self.raw_unpack(UINT_16)))
            elif marker_byte == 0xDA:
                yield dict(tuple(self.unpack(2)) for _ in range(self.raw_unpack(UINT_32)))
            elif 0xB0 <= marker_byte < 0xC0:
                yield Structure(self.raw_unpack(UINT_8), *self.unpack(marker_byte & 0x0F))
            else:
                raise ValueError("Unknown marker byte {:02X}".format(marker_byte))

    def unpack_all(self):
        while self.offset < len(self.data):
            yield next(self.unpack(1))


def unpacked(data, offset=0):
    return next(Packed(data, offset).unpack())


# CHAPTER 2: CONNECTIONS
# ======================
# It's here that we get properly into the protocol. A Bolt connection operates under a
# client-server model whereby a client sends requests to a server and the server responds
# accordingly. There is no mechanism for a server to send a message at any other time, nor should
# client software receive data when no message is expected.

# Connection API Compliance
# -------------------------
# To construct a compatible Connection API, the following function and classes (or their idiomatic
# equivalents) should be implemented. Further details for each of these are given in the reference
# implementations that follow below.
#
# connect(address, connection_settings) -> Connection   // Establish a connection to a Bolt server.
# ConnectionSettings(user, password, user_agent)        // Holder for connection settings.
# Connection(socket, connection_settings)               // Active connection to a Bolt server.
# Request(description, *message)                        // Client request message.
# Response()                                            // Basic response handler.
#   QueryResponse()                                     // Query response handler.
#     QueryStreamResponse()                             // Data stream response handler.
# Failure()                                             // Exception signalling request failure.
# ProtocolError()                                       // Exception signalling protocol breakdown.

# The Handshake
# -------------
# On successfully establishing a TCP connection to a listening server, the client should send a
# fixed sequence of four bytes in order to declare to the server that it intends to speak Bolt.
#
# Fun fact: the four "magic" bytes are supposed to spell "GO GO BOLT!" when written in hex and
# read through squinted eyes. Other hilarious phrases were considered and rejected.
#
BOLT = b"\x60\x60\xB0\x17"

# After the protocol announcement comes version negotiation. The client proposes to the server up
# to four protocol versions that it is able to support in order of preference. Each protocol
# version is a 32-bit unsigned integer and zeroes are used to fill the gaps if the client knows
# fewer than four versions.
#
# In response, the server replies with a single version number on which it agrees. If for some
# reason it can't agree, it returns a zero and disconnects immediately.
#
# Now that you've read and absorbed all that, push it to the back of your brain. As there currently
# exists only one version of Bolt, the negotiation turns out to be far more simple than that. The
# server and client send the following to each other:
#
#   C: 00:00:00:01 00:00:00:00 00:00:00:00 00:00:00:00
#   S: 00:00:00:01
#
# And that's it. If the server responds with anything else, hang up and get in touch. We may have a
# problem.
#

# Messaging
# ---------
# Once we've negotiated the (one and only) protocol version, we fall into the regular protocol
# exchange. This consists of request and response messages, each of which is serialised as a
# PackStream structure with a unique tag byte.
#
# The client sends messages from the selection below:
MAX_BOLT_VERSION = 4
CLIENT = [None] * (MAX_BOLT_VERSION + 1)
CLIENT[1] = {
    "INIT": 0x01,               # INIT <user_agent> <auth_token>
                                # -> SUCCESS - connection initialised
                                # -> FAILURE - init failed, disconnect (reconnect to retry)
                                #
                                # Initialisation is carried out once per connection, immediately
                                # after version negotiation. Before this, no other messages may
                                # validly be exchanged. INIT bundles with it two pieces of data:
                                # a user agent string and a map of auth information. More detail on
                                # on this can be found in the ConnectionSettings class below.

    "ACK_FAILURE": 0x0E,        # ACK_FAILURE
                                # -> SUCCESS - failure acknowledged
                                # -> FAILURE - protocol error, disconnect
                                #
                                # When a FAILURE occurs, no further actions may be carried out
                                # until that failure has been acknowledged by the client. This
                                # is a safety mechanism to prevent actions from being carried out
                                # by the server when several requests have been optimistically sent
                                # at the same time.

    "RESET": 0x0F,              # RESET
                                # -> SUCCESS - connection reset
                                # -> FAILURE - protocol error, disconnect
                                #
                                # A RESET is used to clear the connection state back to how it was
                                # immediately following initialisation. Specifically, any
                                # outstanding failure will be acknowledged, any result stream will
                                # be discarded and any transaction will be rolled back. This is
                                # used primarily by the connection pool.

    "RUN": 0x10,                # RUN <statement> <parameters>
                                # -> SUCCESS - statement accepted
                                # -> FAILURE - statement not accepted
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # RUN is used to execute a Cypher statement and is paired with
                                # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                                # results respectively.
                                # TODO

    "DISCARD_ALL": 0x2F,        # DISCARD_ALL
                                # -> SUCCESS - result discarded
                                # -> FAILURE - no result to discard
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO

    "PULL_ALL": 0x3F,           # PULL_ALL
                                # .. [RECORD*] - zero or more RECORDS may be returned first
                                # -> SUCCESS - result complete
                                # -> FAILURE - no result to pull
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO
}
CLIENT[2] = CLIENT[1]
CLIENT[3] = {
    "HELLO": 0x01,              # HELLO <headers>
                                # -> SUCCESS - connection initialised
                                # -> FAILURE - init failed, disconnect (reconnect to retry)
                                #
                                # Initialisation is carried out once per connection, immediately
                                # after version negotiation. Before this, no other messages may
                                # validly be exchanged. INIT bundles with it two pieces of data:
                                # a user agent string and a map of auth information. More detail on
                                # on this can be found in the ConnectionSettings class below.

    "GOODBYE": 0x02,            # GOODBYE
                                # TODO

    "RESET": 0x0F,              # RESET
                                # -> SUCCESS - connection reset
                                # -> FAILURE - protocol error, disconnect
                                #
                                # A RESET is used to clear the connection state back to how it was
                                # immediately following initialisation. Specifically, any
                                # outstanding failure will be acknowledged, any result stream will
                                # be discarded and any transaction will be rolled back. This is
                                # used primarily by the connection pool.

    "RUN": 0x10,                # RUN <statement> <parameters>
                                # -> SUCCESS - statement accepted
                                # -> FAILURE - statement not accepted
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # RUN is used to execute a Cypher statement and is paired with
                                # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                                # results respectively.
                                # TODO

    "BEGIN": 0x11,              # BEGIN
                                # TODO

    "COMMIT": 0x12,             # COMMIT
                                # TODO

    "ROLLBACK": 0x13,           # ROLLBACK
                                # TODO

    "DISCARD_ALL": 0x2F,        # DISCARD_ALL
                                # -> SUCCESS - result discarded
                                # -> FAILURE - no result to discard
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO

    "PULL_ALL": 0x3F,           # PULL_ALL
                                # .. [RECORD*] - zero or more RECORDS may be returned first
                                # -> SUCCESS - result complete
                                # -> FAILURE - no result to pull
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO
}
CLIENT[4] = {
    "HELLO": 0x01,              # HELLO <headers>
                                # -> SUCCESS - connection initialised
                                # -> FAILURE - init failed, disconnect (reconnect to retry)
                                #
                                # Initialisation is carried out once per connection, immediately
                                # after version negotiation. Before this, no other messages may
                                # validly be exchanged. INIT bundles with it two pieces of data:
                                # a user agent string and a map of auth information. More detail on
                                # on this can be found in the ConnectionSettings class below.

    "GOODBYE": 0x02,            # GOODBYE
                                # TODO

    "RESET": 0x0F,              # RESET
                                # -> SUCCESS - connection reset
                                # -> FAILURE - protocol error, disconnect
                                #
                                # A RESET is used to clear the connection state back to how it was
                                # immediately following initialisation. Specifically, any
                                # outstanding failure will be acknowledged, any result stream will
                                # be discarded and any transaction will be rolled back. This is
                                # used primarily by the connection pool.

    "RUN": 0x10,                # RUN <statement> <parameters>
                                # -> SUCCESS - statement accepted
                                # -> FAILURE - statement not accepted
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # RUN is used to execute a Cypher statement and is paired with
                                # either PULL_ALL or DISCARD_ALL to retrieve or throw away the
                                # results respectively.
                                # TODO

    "BEGIN": 0x11,              # BEGIN
                                # TODO

    "COMMIT": 0x12,             # COMMIT
                                # TODO

    "ROLLBACK": 0x13,           # ROLLBACK
                                # TODO

    "DISCARD": 0x2F,            # DISCARD <n>
                                # -> SUCCESS - result discarded
                                # -> FAILURE - no result to discard
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO

    "PULL": 0x3F,               # PULL <n>
                                # .. [RECORD*] - zero or more RECORDS may be returned first
                                # -> SUCCESS - result complete
                                # -> FAILURE - no result to pull
                                # -> IGNORED - request ignored (due to prior failure)
                                #
                                # TODO
}
#
# The server responds with one or more of these for each request:
SERVER = [None] * (MAX_BOLT_VERSION + 1)
SERVER[1] = {
    "SUCCESS": 0x70,            # SUCCESS <metadata>
    "RECORD": 0x71,             # RECORD <value>
    "IGNORED": 0x7E,            # IGNORED <metadata>
    "FAILURE": 0x7F,            # FAILURE <metadata>
}
SERVER[4] = SERVER[3] = SERVER[2] = SERVER[1]

DEFAULT_USER_AGENT = "boltkit/0.0"
MAX_CHUNK_SIZE = 65535
SOCKET_TIMEOUT = 5


log = getLogger("bolt.client")


# TODO: replace with Connection.open
def connect(address, **settings):
    """ Establish a connection to a Bolt server. It is here that we create a low-level socket
    connection and carry out version negotiation. Following this (and assuming success) a
    Connection instance will be returned.  This Connection takes ownership of the underlying
    socket and is subsequently responsible for managing its lifecycle.

    Args:
        address: A tuple of host and port, such as ("127.0.0.1", 7687).
        settings: Settings for initialising the connection once established.

    Returns:
        A connection to the Bolt server.

    Raises:
        ProtocolError: if the protocol version could not be negotiated.
    """

    return Connection.open(address, **settings)


class Connection:
    """ The Connection wraps a socket through which protocol messages are sent and received. The
    socket is owned by this Connection instance...
    """

    @classmethod
    def open(cls, address, **settings):
        """ Establish a connection to a Bolt server. It is here that we create a low-level socket
        connection and carry out version negotiation. Following this (and assuming success) a
        Connection instance will be returned.  This Connection takes ownership of the underlying
        socket and is subsequently responsible for managing its lifecycle.

        Args:
            address: A tuple of host and port, such as ("127.0.0.1", 7687).
            settings: Settings for initialising the connection once established.

        Returns:
            A connection to the Bolt server.

        Raises:
            ProtocolError: if the protocol version could not be negotiated.
        """

        # Establish a connection to the host and port specified
        log.debug("~~ <CONNECT> %r", address)
        socket = create_connection(address)
        socket.settimeout(SOCKET_TIMEOUT)

        log.debug("C: <BOLT> b'\\x%02x\\x%02x\\x%02x\\x%02x'", *BOLT)
        socket.sendall(BOLT)

        # Establish which protocol versions we want to attempt to use
        bolt_versions = settings.get("bolt_versions")
        if not bolt_versions:
            bolt_versions = sorted([v for v, x in enumerate(CLIENT) if x is not None], reverse=True)
        # Raise an error if we're including any non-supported versions
        if any(v < 0 or v > MAX_BOLT_VERSION for v in bolt_versions):
            raise ProtocolError("Unknown Bolt versions in %r" % bolt_versions)
        # Ensure we send exactly 4 versions, padding with zeroes if necessary
        bolt_versions = (list(bolt_versions) + [0, 0, 0, 0])[:4]
        # Send the protocol versions to the server
        log.debug("C: <VERSION> %r", bolt_versions)
        socket.sendall(b"".join(raw_pack(UINT_32, version) for version in bolt_versions))

        # Handle the handshake response
        raw_bolt_version = socket.recv(4)
        bolt_version, = raw_unpack(UINT_32, raw_bolt_version)
        log.debug("S: <VERSION> %d", bolt_version)

        if bolt_version > 0 and bolt_version in bolt_versions:
            return cls(socket, bolt_version, **settings)
        else:
            log.error("~~ <CLOSE> Could not negotiate protocol version")
            socket.close()
            raise ProtocolError("Could not negotiate protocol version")

    def __init__(self, socket, bolt_version, **settings):
        self.socket = socket
        self.bolt_version = bolt_version
        self.requests = []
        self.responses = []
        user = settings.get("user", "neo4j")
        password = settings.get("password", "password")
        user_agent = settings.get("user_agent", DEFAULT_USER_AGENT)
        if bolt_version >= 3:
            args = {
                "scheme": "basic",
                "principal": user,
                "credentials": password,
                "user_agent": user_agent,
            }
            log.debug("C: HELLO %r" % dict(args, credentials="..."))
            request = Structure(CLIENT[self.bolt_version]["HELLO"], args)
        else:
            auth_token = {
                "scheme": "basic",
                "principal": settings.get("user", "neo4j"),
                "credentials": settings.get("password", "password"),
            }
            log.debug("C: INIT %r %r", user_agent, dict(auth_token, credentials="..."))
            request = Structure(CLIENT[self.bolt_version]["INIT"], user_agent, auth_token)
        self.requests.append(request)
        response = Response(self.bolt_version)
        self.responses.append(response)
        self.send_all()
        self.fetch_all()
        self.server_agent = response.metadata["server"]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        log.debug("~~ <CLOSE>")
        self.socket.close()

    def reset(self):
        log.debug("C: RESET")
        self.requests.append(Structure(CLIENT[self.bolt_version]["RESET"]))
        self.send_all()
        response = Response(self.bolt_version)
        self.responses.append(response)
        while not response.complete():
            self.fetch_one()

    def run(self, statement, parameters=None, metadata=None):
        parameters = parameters or {}
        metadata = metadata or {}
        if self.bolt_version >= 3:
            log.debug("C: RUN %r %r %r", statement, parameters, metadata)
            run = Structure(CLIENT[self.bolt_version]["RUN"], statement, parameters, metadata)
        elif metadata:
            raise ProtocolError("RUN metadata is not available in Bolt v%d" % self.bolt_version)
        else:
            log.debug("C: RUN %r %r", statement, parameters)
            run = Structure(CLIENT[self.bolt_version]["RUN"], statement, parameters)
        self.requests.append(run)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def discard(self, n):
        if self.bolt_version >= 4:
            log.debug("C: DISCARD %r", n)
            self.requests.append(Structure(CLIENT[self.bolt_version]["DISCARD"], n))
        elif n >= 0:
            raise ProtocolError("DISCARD <n> is not available in Bolt v%d" % self.bolt_version)
        else:
            log.debug("C: DISCARD_ALL")
            self.requests.append(Structure(CLIENT[self.bolt_version]["DISCARD_ALL"]))
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def pull(self, n, records):
        if self.bolt_version >= 4:
            log.debug("C: PULL %r", n)
            self.requests.append(Structure(CLIENT[self.bolt_version]["PULL"], n))
        elif n >= 0:
            raise ProtocolError("PULL <n> is not available in Bolt v%d" % self.bolt_version)
        else:
            log.debug("C: PULL_ALL")
            self.requests.append(Structure(CLIENT[self.bolt_version]["PULL_ALL"]))
        response = QueryResponse(self.bolt_version, records)
        self.responses.append(response)
        return response

    def begin(self, metadata=None):
        metadata = metadata or {}
        if self.bolt_version >= 3:
            log.debug("C: BEGIN %r", metadata)
            self.requests.append(Structure(CLIENT[self.bolt_version]["BEGIN"], metadata))
        else:
            raise ProtocolError("BEGIN is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def commit(self):
        if self.bolt_version >= 3:
            log.debug("C: COMMIT")
            self.requests.append(Structure(CLIENT[self.bolt_version]["COMMIT"]))
        else:
            raise ProtocolError("COMMIT is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def rollback(self):
        if self.bolt_version >= 3:
            log.debug("C: ROLLBACK")
            self.requests.append(Structure(CLIENT[self.bolt_version]["ROLLBACK"]))
        else:
            raise ProtocolError("ROLLBACK is not available in Bolt v%d" % self.bolt_version)
        response = QueryResponse(self.bolt_version)
        self.responses.append(response)
        return response

    def send_all(self):
        """ Send all pending request messages to the server.
        """
        if not self.requests:
            return
        data = []
        while self.requests:
            request = self.requests.pop(0)
            request_data = packed(request)
            for offset in range(0, len(request_data), MAX_CHUNK_SIZE):
                end = offset + MAX_CHUNK_SIZE
                chunk = request_data[offset:end]
                data.append(raw_pack(UINT_16, len(chunk)))
                data.append(chunk)
            data.append(raw_pack(UINT_16, 0))
        self.socket.sendall(b"".join(data))

    def fetch_one(self):
        """ Receive exactly one response message from the server. This method blocks until either a
        message arrives or the connection is terminated.
        """

        # Receive chunks of data until chunk_size == 0
        data = []
        chunk_size = -1
        while chunk_size != 0:
            chunk_size, = raw_unpack(UINT_16, self.socket.recv(2))
            if chunk_size > 0:
                data.append(self.socket.recv(chunk_size))
        message = unpacked(b"".join(data))

        # Handle message
        response = self.responses[0]
        try:
            response.on_message(message.tag, *message.fields)
        except Failure as failure:
            if isinstance(failure.response, QueryResponse):
                self.reset()
            else:
                self.close()
            raise
        finally:
            if response.complete():
                self.responses.pop(0)

    def fetch_summary(self):
        """ Fetch all messages up to and including the next summary message.
        """
        response = self.responses[0]
        while not response.complete():
            self.fetch_one()

    def fetch_all(self):
        """ Fetch all messages from all outstanding responses.
        """
        while self.responses:
            self.fetch_summary()


class Response:
    # Basic request that expects SUCCESS or FAILURE back, e.g. RESET

    def __init__(self, bolt_version):
        self.bolt_version = bolt_version
        self.metadata = None

    def complete(self):
        return self.metadata is not None

    def on_success(self, data):
        log.debug("S: SUCCESS %r", data)
        self.metadata = data

    def on_failure(self, data):
        log.debug("S: FAILURE %r", data)
        self.metadata = data
        raise Failure(self)

    def on_message(self, tag, data=None):
        if tag == SERVER[self.bolt_version]["SUCCESS"]:
            self.on_success(data)
        elif tag == SERVER[self.bolt_version]["FAILURE"]:
            self.on_failure(data)
        else:
            raise ProtocolError("Unexpected summary message with tag %02X received" % tag)


class QueryResponse(Response):
    # Can also be IGNORED (RUN, DISCARD_ALL)

    def __init__(self, bolt_version, records=None):
        super().__init__(bolt_version)
        self.ignored = False
        self.records = records

    def on_ignored(self, data):
        log.debug("S: IGNORED %r", data)
        self.ignored = True
        self.metadata = data

    def on_record(self, data):
        log.debug("S: RECORD %r", data)
        if self.records is not None:
            self.records.append(data)

    def on_message(self, tag, data=None):
        if tag == SERVER[self.bolt_version]["RECORD"]:
            self.on_record(data)
        elif tag == SERVER[self.bolt_version]["IGNORED"]:
            self.on_ignored(data)
        else:
            super(QueryResponse, self).on_message(tag, data)


class Failure(Exception):

    def __init__(self, response):
        super(Failure, self).__init__(response.metadata["message"])
        self.response = response
        self.code = response.metadata["code"]


class ProtocolError(Exception):

    pass
