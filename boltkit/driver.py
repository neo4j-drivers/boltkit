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

1. Low-level data serialisation. For this, we use a custom serialisation format called
   PackStream. While the design of this format is inspired heavily by MessagePack, it is not
   compatible with it. PackStream provides a type system that is fully compatible with the Cypher
   type system used by Neo4j and also takes extension data types in a different direction to
   MessagePack. More on PackStream shortly.
2. Bolt messaging. At its heart, the Bolt protocol provides a stateful request-response
   mechanism. Each request consists of a textual statement plus a map or dictionary of
   parameters; each response is comprised of a stream of content plus some summary metadata.
   Message pipelining comes for free: a Bolt server will queue requests and respond to them in
   the same order in which they were received. A Bolt client therefore has a degree of
   flexibility in how and when it sends requests and how and when it gathers the responses.
3. The Session API. Compliant drivers adhere to a standardised API design that sits atop the
   messaging layer. This provides a consistent vocabulary and pattern of usage for application
   developers, regardless of language. Though this is of course a minimum. Any driver author
   should feel free to innovate around this and provide any amount of language-idiomatic extras
   that are appropriate or desirable.

"""

# You'll need to make sure you have the following items handy...
from collections import deque
from json import dumps as json_dumps
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

    :param data: input byte data as `bytes` or a `bytearray`
    :return: textual representation of the input data
    """
    return ":".join("{:02X}".format(b) for b in bytearray(data))


def pack(*values):
    """ This function provides PackStream values-to-bytes functionality, a process known as
    "packing". The signature of the method permits any number of values to be provided as
    positional arguments. Each will be serialised in order into the output byte stream.

        >>> from boltkit.driver import pack
        >>> h(pack(1))
        '01'
        >>> h(pack(1234))
        'C9:04:D2'
        >>> h(pack(6.283185307179586))
        'C1:40:19:21:FB:54:44:2D:18'
        >>> h(pack(False))
        'C2'
        >>> h(pack("Übergröße"))
        '8C:C3:9C:62:65:72:67:72:C3:B6:C3:9F:65'
        >>> h(pack([1, True, 3.14, "fünf"]))
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


    :param values: series of values to pack
    :return: `bytes` serialisation of the packed values
    """

    # First, let's define somewhere to collect the individual byte pieces and grab a couple of
    # handles to commonly-used methods.
    #
    packed = []
    append = packed.append
    extend = packed.extend

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
            data = value.encode("UTF-8")
            size = len(data)
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
            append(data)

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
            extend(map(pack, value))

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
            extend(pack(k, v) for k, v in value.items())

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
            extend(map(pack, fields))

        # For anything else, we'll just raise an error as we don't know how to encode it.
        #
        else:
            raise TypeError("Cannot pack objects of type %s" % type(value).__name__)

    # Finally, we can glue all the individual pieces together and return the full byte
    # representation of the original values.
    #
    return b"".join(packed)


class Packed(object):

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


def unpack(data, offset=0):
    return next(Packed(data, offset).unpack())


# CHAPTER 2: BOLT MESSAGING
# =========================

# TODO (protocol logging occurs here)

DEFAULT_PORT = 7687
MAGIC = b"\x60\x60\xB0\x17"
MAX_CHUNK_SIZE = 65535

# Dictionary of Bolt message names mapped to signature bytes
BOLT = {
    "INIT": 0x01,               # 0000 0001 // INIT <user_agent> <auth_token>
    "ACK_FAILURE": 0x0E,        # 0000 1110 // ACK_FAILURE
    "RESET": 0x0F,              # 0000 1111 // RESET
    "RUN": 0x10,                # 0001 0000 // RUN <statement> <parameters>
    "DISCARD_ALL": 0x2F,        # 0010 1111 // DISCARD_ALL
    "PULL_ALL": 0x3F,           # 0011 1111 // PULL_ALL
    "SUCCESS": 0x70,            # 0111 0000 // SUCCESS <metadata>
    "RECORD": 0x71,             # 0111 0001 // RECORD <value>
    "IGNORED": 0x7E,            # 0111 1110 // IGNORED <metadata>
    "FAILURE": 0x7F,            # 0111 1111 // FAILURE <metadata>
}


def message_repr(tag, *data):
    message_name = next(key for key, value in BOLT.items() if value == tag)
    return "%s %s" % (message_name, " ".join(map(json_dumps, data)))


def log(text, *args):
    print(text % args)


class ProtocolError(Exception):

    pass


class Failure(Exception):

    def __init__(self, request, **metadata):
        super(Failure, self).__init__(metadata["message"])
        self.code = metadata["code"]
        self.request = request


class Request(object):
    # Basic request that expects SUCCESS or FAILURE back: INIT, ACK_FAILURE or RESET

    metadata = None
    complete = False

    def __init__(self, *message):
        self.packed = pack(message)
        self.repr = message_repr(*message)

    def __repr__(self):
        return self.repr

    def on_record(self, data):
        raise ProtocolError("Response should not contain records")

    def on_success(self, data):
        self.metadata = data
        self.complete = True

    def on_failure(self, data):
        self.complete = True
        raise Failure(self, **data)

    def on_ignored(self, data):
        raise ProtocolError("Request should not be ignored")

    def on_message(self, tag, data=None):
        if tag == BOLT["RECORD"]:
            self.on_record(data)
        elif tag == BOLT["SUCCESS"]:
            self.on_success(data)
        elif tag == BOLT["FAILURE"]:
            self.on_failure(data)
        elif tag == BOLT["IGNORED"]:
            self.on_ignored(data)
        else:
            raise ProtocolError("Unexpected response message")


class QueryRequest(Request):
    # Can be ignored (RUN, DISCARD_ALL)

    ignored = False

    def on_ignored(self, data):
        self.ignored = data
        self.complete = True


class QueryStreamRequest(QueryRequest):
    # Can return records (PULL_ALL)

    def __init__(self, records, *message):
        super(QueryStreamRequest, self).__init__(*message)
        self.records = records

    def on_record(self, data):
        self.records.append(data or [])


class Connection(object):
    """ Server connection through which all protocol messages
    are sent and received.
    """

    def __init__(self, socket):
        self.socket = socket
        self.outgoing = deque()
        self.incoming = deque()

    def init(self, user_agent, auth_token):
        init = Request(BOLT["INIT"], user_agent, auth_token)
        self.outgoing.append(init)
        return self.sync(init)

    def reset(self):
        reset = Request(BOLT["RESET"])
        self.outgoing.append(reset)
        return self.sync(reset)

    def add_statement(self, statement, parameters, records=None):
        # returns pair of requests
        run = QueryRequest(BOLT["RUN"], statement, parameters)
        if records is None:
            stream = QueryRequest(BOLT["DISCARD_ALL"])
        else:
            stream = QueryStreamRequest(records, BOLT["PULL_ALL"])
        self.outgoing.extend([run, stream])
        return run, stream

    def dispatch(self):
        """ Send all pending requests to the server.
        """
        data = []

        while self.outgoing:
            request = self.outgoing.popleft()
            log("C: %r" % request)
            for offset in range(0, len(request.packed), MAX_CHUNK_SIZE):
                end = offset + MAX_CHUNK_SIZE
                chunk = request.packed[offset:end]
                data.append(raw_pack(UINT_16, len(chunk)))
                data.append(chunk)
            data.append(raw_pack(UINT_16, 0))
            self.incoming.append(request)

        if data:
            self.socket.sendall(b"".join(data))

    def fetch(self):
        """ Receive exactly one message from an open socket
        """

        # Receive chunks of data until chunk_size == 0
        data = []
        chunk_size = -1
        while chunk_size != 0:
            chunk_size, = raw_unpack(UINT_16, self.socket.recv(2))
            if chunk_size > 0:
                data.append(self.socket.recv(chunk_size))
        message = unpack(b"".join(data))
        log("S: %s" % message_repr(*message))

        # Handle message
        request = self.incoming[0]
        try:
            request.on_message(*message)
        except Failure as failure:
            if isinstance(failure.request, QueryRequest):
                self.outgoing.append(Request(BOLT["ACK_FAILURE"]))
            else:
                self.close()
            raise
        finally:
            if request.complete:
                self.incoming.popleft()
        return not request.complete

    def sync(self, request):
        self.dispatch()
        while request in self.incoming:
            while self.fetch():
                pass
        return request

    def close(self):
        log("~~ [DISCONNECT]")
        self.socket.close()


def connect(address):
    """ Connect and perform a handshake in order to return a valid
    Connection object, assuming a protocol version can be agreed.
    """

    # Establish a connection to the host and port specified
    log("~~ [CONNECT] %r", address)
    socket = create_connection(address)

    log("C: [MAGIC] %s", h(MAGIC))
    socket.sendall(MAGIC)

    # Send details of the protocol versions supported
    supported_versions = [1, 0, 0, 0]
    data = b"".join(raw_pack(UINT_32, version) for version in supported_versions)
    log("C: [HANDSHAKE] %s", h(data))
    socket.sendall(data)

    # Handle the handshake response
    data = socket.recv(4)
    log("S: [HANDSHAKE] %s", h(data))
    agreed_version, = raw_unpack(UINT_32, data)
    if agreed_version == 1:
        return Connection(socket)
    else:
        log("~~ [DISCONNECT] Could not negotiate protocol version")
        socket.close()
        raise RuntimeError("Could not negotiate protocol version")


# CHAPTER 3: SESSIONS
# ===================


class ConnectionPool(object):

    def __init__(self, address, user_agent, auth_token):
        self.address = address
        self.user_agent = user_agent
        self.auth_token = auth_token
        self.connections = set()

    def acquire(self):
        """ Acquire connection from pool
        """
        try:
            connection = self.connections.pop()
        except KeyError:
            connection = connect(self.address)
            try:
                connection.init(self.user_agent, self.auth_token)
            except Failure:
                connection.close()
                raise ProtocolError("Failed to init connection")
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


class Driver(object):

    def __init__(self, uri, user_agent, auth_token):
        parsed = urlparse(uri)
        if parsed.scheme == "bolt":
            address = parsed.hostname, parsed.port or DEFAULT_PORT
            self.connection_pool = ConnectionPool(address, user_agent, auth_token)
        else:
            raise ValueError("Unsupported URI scheme %r" % parsed.scheme)

    def session(self):
        return Session(self.connection_pool)

    def close(self):
        self.connection_pool.close()


class Session(object):

    connection = None  # Declared here as the destructor references it

    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = self.connection_pool.acquire()

    def __del__(self):
        self.close()

    def run(self, statement, parameters=None):
        return StatementResult(self.connection, statement, parameters or {})

    def close(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None


class StatementResult(object):

    def __init__(self, connection, statement, parameters):
        self.connection = connection
        self.records = deque()
        self.run, self.stream = connection.add_statement(statement, parameters, self.records)

    def keys(self):
        self.connection.sync(self.run)
        return self.run.metadata.get("fields", [])

    def buffer(self):
        self.connection.sync(self.stream)

    def consume(self):
        self.buffer()
        self.records.clear()

    def summary(self):
        self.buffer()
        return self.stream.metadata


def main():
    from sys import argv
    driver = Driver(u"bolt://localhost:7687", u"DemoDriver/1.1",
                    {u"scheme": u"basic", u"principal": u"neo4j", u"credentials": u"password"})
    session = driver.session()
    session.run(argv[1]).consume()
    driver.close()


if __name__ == "__main__":
    main()
