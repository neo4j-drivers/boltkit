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
from collections import deque
from json import dumps as json_dumps
from logging import getLogger
from socket import create_connection
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import version_info
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


# Before we go any further, we just need to do a bit of magic to work around some Python domestic
# matters. If you care, Python 2 and Python 3 don't really get on. That is, they don't really
# agree on how to raise the kids. So we just need to work out for ourselves what is actually an
# integer and what is actually a (Unicode) string.
#
if version_info >= (3,):
    integer = int
    string = str
else:
    integer = (int, long)
    string = unicode
#
# OK, that's done. Sorry about that. I *did* say there would be no boilerplate. What I actually
# meant was that there wouldn't be *very much* boilerplate. Right, let's get on with the
# interesting stuff...


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
#   - Structure (composite set of values with a type signature)
#
# Neither unsigned integers nor byte arrays are supported but may be added in a future version of
# the format. Note that 32-bit floating point numbers are also not supported. This is a deliberate
# decision and these won't be added in any future version.


# Oh, by the way, we use hexadecimal a lot here. If you're not familiar with that, you might want
# to take a short break and hop over to Wikipedia to read up about it before going much further...


def h(data):
    """ A small helper function to translate byte data into a human-readable hexadecimal
    representation. Each byte in the input data is converted into a two-character hexadecimal
    string and is joined to its neighbours with a colon character.

    This function is not essential to driver-building but is a great help when debugging,
    logging and writing doctests.

        >>> from boltkit.driver import h
        >>> h(b"\x03A~")
        '03:41:7E'

    Args:
        data: Input byte data as `bytes` or a `bytearray`.

    Returns:
        A textual representation of the input data.
    """
    return ":".join("{:02X}".format(b) for b in bytearray(data))


def packed(*values):
    """ This function provides PackStream values-to-bytes functionality, a process known as
    "packing". The signature of the method permits any number of values to be provided as
    positional arguments. Each will be serialised in order into the output byte stream.

        >>> from boltkit.driver import packed
        >>> h(packed(1))
        '01'
        >>> h(packed(1234))
        'C9:04:D2'
        >>> h(packed(6.283185307179586))
        'C1:40:19:21:FB:54:44:2D:18'
        >>> h(packed(False))
        'C2'
        >>> h(packed("Übergröße"))
        '8C:C3:9C:62:65:72:67:72:C3:B6:C3:9F:65'
        >>> h(packed([1, True, 3.14, "fünf"]))
        '94:01:C3:C1:40:09:1E:B8:51:EB:85:1F:85:66:C3:BC:6E:66'

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
        elif isinstance(value, integer):
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
        elif isinstance(value, string):
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
        #     [1, 2, 3, ... 40] -> D4:28:00:01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F
        #                          10:11:12:13:14:15:16:17:18:19:1A:1B:1C:1D:1E:1F:20:21
        #                          22:23:24:25:26:27
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
        # signature followed by a sequence of fields, each an individual value. The size of a
        # structure is measured as the number of fields and not the total byte size. This count
        # does not include the signature. The markers used to denote a  structure are described in
        # the table below:
        #
        #   Marker | Size                                        | Maximum size
        #  ========|=============================================|=======================
        #   B0..BF | contained within low-order nibble of marker | 15 fields
        #   DC     | 8-bit big-endian unsigned integer           | 255 fields
        #   DD     | 16-bit big-endian unsigned integer          | 65 535 fields
        #
        # The signature byte is used to identify the type or class of the structure. Signature
        # bytes may hold any value between 0 and +127. Bytes with the high bit set are reserved
        # for future expansion. For structures containing fewer than 16 fields, the marker byte
        # should contain the high-order nibble 'B' (binary 1011) followed by a low-order nibble
        # containing the size. The marker is immediately followed by the signature byte and the
        # field values.
        #
        # For structures containing 16 fields or more, the marker DC or DD should be used,
        # depending on scale. This marker is followed by the size, the signature byte and the
        # fields, serialised in order. Examples follow below:
        #
        #     B3 01 01 02 03  -- Struct(sig=0x01, fields=[1,2,3])
        #     DC 10 7F 01  02 03 04 05  06 07 08 09  00 01 02 03
        #     04 05 06  -- Struct(sig=0x7F, fields=[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6]
        #
        # In this demo, we've chosen to equate a structure with a tuple for simplicity. Here,
        # the first tuple entry denotes the signature and the remainder the fields.
        #
        elif isinstance(value, tuple):
            signature, fields = value[0], value[1:]
            size = len(fields)
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
            append(raw_pack(UINT_8, signature))
            extend(map(packed, fields))

        # For anything else, we'll just raise an error as we don't know how to encode it.
        #
        else:
            raise ValueError("Cannot pack value %r" % value)

    # Finally, we can glue all the individual pieces together and return the full byte
    # representation of the original values.
    #
    return b"".join(data)


class Packed(object):
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
                yield (self.raw_unpack(UINT_8),) + tuple(self.unpack(marker_byte & 0x0F))
            else:
                raise ValueError("Unknown marker byte {:02X}".format(marker_byte))


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
BOLT_VERSION = 1
RAW_BOLT_VERSIONS = b"".join(raw_pack(UINT_32, version) for version in [BOLT_VERSION, 0, 0, 0])

# Messaging
# ---------
# Once we've negotiated the (one and only) protocol version, we fall into the regular protocol
# exchange. This consists of request and response messages, each of which is serialised as a
# PackStream structure with a unique signature byte.
#
# The client sends messages from the selection below:
CLIENT = {
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
#
# The server responds with one or more of these for each request:
SERVER = {
    "SUCCESS": 0x70,            # SUCCESS <metadata>
    "RECORD": 0x71,             # RECORD <value>
    "IGNORED": 0x7E,            # IGNORED <metadata>
    "FAILURE": 0x7F,            # FAILURE <metadata>
}

DEFAULT_USER_AGENT = u"boltkit/0.0"
MAX_CHUNK_SIZE = 65535
SOCKET_TIMEOUT = 5


log = getLogger("boltkit.connection")


def connect(address, connection_settings):
    """ Establish a connection to a Bolt server. It is here that we create a low-level socket
    connection and carry out version negotiation. Following this (and assuming success) a
    Connection instance will be returned.  This Connection takes ownership of the underlying
    socket and is subsequently responsible for managing its lifecycle.

    Args:
        address: A tuple of host and port, such as ("127.0.0.1", 7687).
        connection_settings: Settings for initialising the connection once established.

    Returns:
        A connection to the Bolt server.

    Raises:
        ProtocolError: if the protocol version could not be negotiated.
    """

    # Establish a connection to the host and port specified
    log.info("~~ <CONNECT> \"%s\" %d", *address)
    socket = create_connection(address)
    socket.settimeout(SOCKET_TIMEOUT)

    log.info("C: <BOLT>")
    socket.sendall(BOLT)

    # Send details of the protocol versions supported
    log.info("C: <VERSION> %d", BOLT_VERSION)
    socket.sendall(RAW_BOLT_VERSIONS)

    # Handle the handshake response
    raw_version = socket.recv(4)
    version, = raw_unpack(UINT_32, raw_version)
    log.info("S: <VERSION> %d", version)

    if version == BOLT_VERSION:
        return Connection(socket, connection_settings)
    else:
        log.error("~~ <CLOSE> Could not negotiate protocol version")
        socket.close()
        raise ProtocolError("Could not negotiate protocol version")


class ConnectionSettings(object):

    def __init__(self, user, password, user_agent=None):
        self.user = user
        auth_token = {u"scheme": u"basic", u"principal": user, u"credentials": password}
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.init_request = Request("INIT ...", CLIENT["INIT"], user_agent, auth_token)


class Connection(object):
    """ The Connection wraps a socket through which protocol messages are sent and received. The
    socket is owned by this Connection instance...
    """

    def __init__(self, socket, connection_settings):
        self.socket = socket
        self.requests = deque([connection_settings.init_request])
        self.flush()
        response = Response()
        self.responses = deque([response])
        while not response.complete():
            self.fetch()

    def add_statement(self, statement, parameters, records=None):
        # returns pair of requests
        self.requests.append(Request("RUN %s %s" % (json_dumps(statement), json_dumps(parameters)),
                             CLIENT["RUN"], statement, parameters))
        head = QueryResponse()
        if records is None:
            self.requests.append(DISCARD_ALL_REQUEST)
            tail = QueryResponse()
        else:
            self.requests.append(PULL_ALL_REQUEST)
            tail = QueryStreamResponse(records)
        self.responses.extend([head, tail])
        return head, tail

    def flush(self):
        """ Send all pending request messages to the server.
        """
        if not self.requests:
            return
        data = []
        while self.requests:
            request = self.requests.popleft()
            log.info("C: %s", request.description)
            for offset in range(0, len(request.data), MAX_CHUNK_SIZE):
                end = offset + MAX_CHUNK_SIZE
                chunk = request.data[offset:end]
                data.append(raw_pack(UINT_16, len(chunk)))
                data.append(chunk)
            data.append(raw_pack(UINT_16, 0))
        self.socket.sendall(b"".join(data))

    def fetch(self):
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
            response.on_message(*message)
        except Failure as failure:
            if isinstance(failure.response, QueryResponse):
                self.requests.append(ACK_FAILURE_REQUEST)
                self.responses.append(Response())
            else:
                self.close()
            raise
        finally:
            if response.complete():
                self.responses.popleft()

    def reset(self):
        self.requests.append(RESET_REQUEST)
        self.flush()
        response = Response()
        self.responses.append(response)
        while not response.complete():
            self.fetch()

    def close(self):
        log.info("~~ <CLOSE>")
        self.socket.close()


class ConnectionPool(object):

    connections = None

    def __init__(self, address, connection_settings):
        self.address = address
        self.connection_settings = connection_settings
        self.connections = set()

    def __del__(self):
        self.close()

    def acquire(self):
        """ Acquire connection from pool
        """
        try:
            connection = self.connections.pop()
        except KeyError:
            connection = connect(self.address, self.connection_settings)
        return connection

    def release(self, connection):
        """ Release connection back into pool.
        """
        if connection not in self.connections:
            try:
                connection.reset()
            except Failure:
                connection.close()
                raise ProtocolError("Failed to reset connection")
            else:
                self.connections.add(connection)

    def close(self):
        while self.connections:
            self.connections.pop().close()


class Request(object):

    def __init__(self, description, *message):
        self.description = description
        self.data = packed(message)


ACK_FAILURE_REQUEST = Request("ACK_FAILURE", CLIENT["ACK_FAILURE"])
RESET_REQUEST = Request("RESET", CLIENT["RESET"])
DISCARD_ALL_REQUEST = Request("DISCARD_ALL", CLIENT["DISCARD_ALL"])
PULL_ALL_REQUEST = Request("PULL_ALL", CLIENT["PULL_ALL"])


class Response(object):
    # Basic request that expects SUCCESS or FAILURE back: INIT, ACK_FAILURE or RESET

    metadata = None

    def complete(self):
        return self.metadata is not None

    def on_success(self, data):
        log.info("S: SUCCESS %s", json_dumps(data))
        self.metadata = data

    def on_failure(self, data):
        log.info("S: FAILURE %s", json_dumps(data))
        self.metadata = data
        raise Failure(self)

    def on_message(self, tag, data=None):
        if tag == SERVER["SUCCESS"]:
            self.on_success(data)
        elif tag == SERVER["FAILURE"]:
            self.on_failure(data)
        else:
            raise ProtocolError("Unexpected summary message %s received" % h(tag))


class QueryResponse(Response):
    # Can also be IGNORED (RUN, DISCARD_ALL)

    ignored = False

    def on_ignored(self, data):
        log.info("S: IGNORED %s", json_dumps(data))
        self.ignored = True
        self.metadata = data

    def on_message(self, tag, data=None):
        if tag == SERVER["IGNORED"]:
            self.on_ignored(data)
        else:
            super(QueryResponse, self).on_message(tag, data)


class QueryStreamResponse(QueryResponse):
    # Allows RECORD messages back as well (PULL_ALL)

    def __init__(self, records):
        self.records = records

    def on_record(self, data):
        log.info("S: RECORD %s", json_dumps(data))
        self.records.append(data or [])

    def on_message(self, tag, data=None):
        if tag == SERVER["RECORD"]:
            self.on_record(data)
        else:
            super(QueryStreamResponse, self).on_message(tag, data)


class Failure(Exception):

    def __init__(self, response):
        super(Failure, self).__init__(response.metadata["message"])
        self.response = response
        self.code = response.metadata["code"]


class ProtocolError(Exception):

    pass


# CHAPTER 3: SESSION API
# ======================

# TODO: Transaction
# TODO: Node
# TODO: Relationship
# TODO: Path


class Driver(object):

    port = 7687
    user_agent = u"boltkit-driver/1.0.0"

    connection_pool = None  # Declared here as the destructor references it

    def __init__(self, uri, **config):
        parsed = urlparse(uri)
        if parsed.scheme == "bolt":
            if parsed.port:
                self.port = parsed.port
            address = parsed.hostname, self.port
            self.user_agent = config.get("user_agent", self.user_agent)
            self.connection_settings = ConnectionSettings(config["user"], config["password"],
                                                          self.user_agent)
            self.connection_pool = ConnectionPool(address, self.connection_settings)
        else:
            raise ValueError("Unsupported URI scheme %r" % parsed.scheme)

    def __del__(self):
        self.close()

    def session(self):
        return Session(self.connection_pool)

    def close(self):
        if self.connection_pool:
            self.connection_pool.close()


class Session(object):

    connection = None  # Declared here as the destructor references it

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = self.connection_pool.acquire()

    def __del__(self):
        self.close()

    def run(self, statement, parameters=None):
        return Result(self.connection, statement, parameters or {})

    def reset(self):
        self.connection.reset()

    def close(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None


class Result(object):
    # API spec: can be named Result or ***Result
    #           will generally define idiomatic iteration atop forward() and current()

    def __init__(self, connection, statement, parameters):
        # shares a connection, never closes or explicitly releases it
        self.connection = connection
        # additions made by Response, removals here
        self.records = deque([None])
        self.head, self.tail = connection.add_statement(statement, parameters, self.records)

    def keys(self):
        self.connection.flush()
        while not self.head.complete():
            self.connection.fetch()
        return self.head.metadata.get("fields", [])

    def current(self):
        return self.records[0]

    def forward(self):
        if len(self.records) > 1:
            self.records.popleft()
            return True
        elif self.tail.complete():
            return False
        else:
            self.connection.flush()
            while len(self.records) == 1 and not self.tail.complete():
                self.connection.fetch()
            return self.forward()

    def buffer(self):
        self.connection.flush()
        while not self.tail.complete():
            self.connection.fetch()

    def summary(self):
        self.buffer()
        return self.tail.metadata
