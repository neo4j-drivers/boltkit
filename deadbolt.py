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
Deadbolt is a stub Bolt server
"""

from collections import deque
from json import loads as json_loads, JSONDecoder, JSONDecodeError
from select import select
from socket import socket, SOL_SOCKET, SO_REUSEADDR
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import argv, stderr, version_info, exit
from threading import Thread


if version_info >= (3,):
    integer = int
    string = str
else:
    integer = (int, long)
    string = unicode


INT_8 = ">b"        # signed 8-bit integer (two's complement)
INT_16 = ">h"       # signed 16-bit integer (two's complement)
INT_32 = ">i"       # signed 32-bit integer (two's complement)
INT_64 = ">q"       # signed 64-bit integer (two's complement)
UINT_8 = ">B"       # unsigned 8-bit integer
UINT_16 = ">H"      # unsigned 16-bit integer
UINT_32 = ">I"      # unsigned 32-bit integer
FLOAT_64 = ">d"     # IEEE double-precision floating-point format


def h(data):
    """ A small helper function to translate byte data into a human-readable hexadecimal
    representation. Each byte in the input data is converted into a two-character hexadecimal
    string and is joined to its neighbours with a colon character.

    This function is not essential to driver-building but is a great help when debugging,
    logging and writing doctests.

        >>> from driver import h
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

        >>> from driver import pack
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
    return "%s %s" % (message_name, " ".join(map(repr, data)))


class Server:

    def __init__(self, address):
        self.address = address
        self.version = 0


class BoltServer(Thread):

    def __init__(self, address, responses=None):
        super(BoltServer, self).__init__()
        self.server = socket()
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server.bind(address)
        self.server.listen()
        self.servers = {self.server: Server(address)}
        self.responses = list(responses or [])
        self.running = True

    def run(self):
        while self.running:
            read_list, _, _ = select(list(self.servers), [], [])
            for sock in read_list:
                self.read(sock)

    def read(self, sock):
        if sock == self.server:
            self.accept(sock)
        elif self.servers[sock].version:
            self.handle_message(sock)
        else:
            self.handshake(sock)

    def accept(self, sock):
        new_sock, address = sock.accept()
        self.servers[new_sock] = Server(address)
        self.log("~~ <ACCEPT> %s -> %s", self.servers[sock].address, self.servers[new_sock].address)

    def handshake(self, sock):
        chunked_data = sock.recv(4)
        self.log("C: <MAGIC>\n     %s", h(chunked_data))
        chunked_data = sock.recv(16)
        self.log("C: <HANDSHAKE>\n     %s", h(chunked_data))
        response = chunked_data[0:4]
        self.log("S: <HANDSHAKE>\n     %s", h(response))
        sock.send(response)
        self.servers[sock].version = 1

    def handle_message(self, sock):
        chunked_data = b""
        message_data = b""
        chunk_size = -1
        debug = []
        while chunk_size != 0:
            chunk_header = sock.recv(2)
            if len(chunk_header) == 0:
                self.log("~~ <CLOSE> %s", self.servers[sock].address)
                del self.servers[sock]
                sock.close()
                self.running = False
                return
            chunked_data += chunk_header
            chunk_size, = raw_unpack(UINT_16, chunk_header)
            if chunk_size > 0:
                chunk = sock.recv(chunk_size)
                chunked_data += chunk
                message_data += chunk
            else:
                chunk = b""
            debug.append("     [%s] %s" % (h(chunk_header), h(chunk)))
        message = unpack(message_data)
        self.log("C: %s\n%s", message_repr(*message), "\n".join(debug))
        self.send_response(sock, *message)

    def send_response(self, sock, *request):
        more = True
        while more:
            response = None
            for rq, rs in self.responses:
                if rq == request:
                    try:
                        response = rs.popleft()
                    except IndexError:
                        pass
                    break
            if response is None:
                response = (BOLT["SUCCESS"], {"fields": []} if request[0] == BOLT["RUN"] else {})
            self.log(self.send_message(sock, *response))
            if response[0] in (BOLT["SUCCESS"], BOLT["FAILURE"], BOLT["IGNORED"]):
                more = False

    def send_message(self, sock, *message):
        data = pack(message)
        chunks = [self.send_chunk(sock, data), self.send_chunk(sock)]
        return "S: %s\n     %s\n     %s" % \
            (message_repr(*message), chunks[0], chunks[1])

    def send_chunk(self, sock, data=b""):
        header = raw_pack(UINT_16, len(data))
        sock.send(header)
        return "[%s] %s" % (h(header), self.send_bytes(sock, data))

    def send_bytes(self, sock, data):
        sock.send(data)
        return h(data)

    def log(self, text, *args):
        stderr.write(text % args)
        stderr.write("\n")

    def log_message(self, peer, signature, *fields):
        self.log("%s: %s", peer, message_repr(signature, *fields))


class BoltCluster:

    def __init__(self, specs):
        self.specs = specs
        self.servers = []
        for spec in self.specs:
            bind_address = ("127.0.0.1", spec.port)
            server = BoltServer(bind_address, spec.responses)
            self.servers.append(server)

    def start(self):
        for server in self.servers:
            server.daemon = True
            server.start()

    def is_alive(self):
        is_alive = False
        for server in self.servers:
            is_alive = is_alive or server.is_alive()
        return is_alive


class ServerSpec:

    def __init__(self, port):
        self.port = port
        self.responses = []

    def add_exchange(self, request, response):
        tag, _, data = response.partition(" ")
        try:
            data = json_loads(data)
        except JSONDecodeError:
            data = None
        if tag == "RECORD":
            response = (BOLT["RECORD"], data or [])
        elif tag == "SUCCESS":
            response = (BOLT["SUCCESS"], data or {})
        elif tag == "FAILURE":
            response = (BOLT["FAILURE"], data or {})
        elif tag == "IGNORED":
            response = (BOLT["IGNORED"], data or {})
        else:
            raise ValueError("Mysterious response data %r" % response)
        for rq, rs in self.responses:
            if rq == request:
                rs.append(response)
                break
        else:
            self.responses.append((request, deque([response])))


def parse_message(message):
    tag, _, data = message.partition(" ")
    parsed = (BOLT[tag],)
    decoder = JSONDecoder()
    while data:
        data = data.lstrip()
        try:
            decoded, end = decoder.raw_decode(data)
        except JSONDecodeError:
            break
        else:
            parsed += (decoded,)
            data = data[end:]
    return parsed


def main():
    if len(argv) < 1:
        print("usage: %s <ports> <script> [<script> ...]" % argv[0])
        exit()
    specs = []
    for i, port_string in enumerate(argv[1].split(":"), start=2):
        spec = ServerSpec(int(port_string))
        try:
            script_file = argv[i]
        except IndexError:
            pass
        else:
            with open(script_file) as script:
                request = None
                for line in script:
                    if line.startswith("C:"):
                        request = parse_message(line[2:].strip())
                    elif line.startswith("S:"):
                        spec.add_exchange(request, line[2:].strip())
        specs.append(spec)
    cluster = BoltCluster(specs)
    cluster.start()
    try:
        while cluster.is_alive():
            pass
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
