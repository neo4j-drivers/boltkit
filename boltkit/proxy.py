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


from argparse import ArgumentParser, RawDescriptionHelpFormatter
from socket import socket, SOL_SOCKET, SO_REUSEADDR
from struct import unpack_from as raw_unpack
from threading import Thread


HELP = """\
Simple proxy server for the Bolt protocol.

Example:

  $ bin/neo4j start     # Neo4j listening on default port 7687
  $ boltproxy --bind-address 0.0.0.0:7777 --server-address localhost:7687
  $ python
  >>> from neo4j import Driver
  >>> dx = Driver("bolt://:7777", auth=("neo4j", "password"), encrypted=False)
  >>> dx.session().run("RETURN 1").single().value()
  1

"""

INT_8 = ">b"        # signed 8-bit integer (two's complement)
INT_16 = ">h"       # signed 16-bit integer (two's complement)
INT_32 = ">i"       # signed 32-bit integer (two's complement)
INT_64 = ">q"       # signed 64-bit integer (two's complement)
UINT_8 = ">B"       # unsigned 8-bit integer
UINT_16 = ">H"      # unsigned 16-bit integer
UINT_32 = ">I"      # unsigned 32-bit integer
FLOAT_64 = ">d"     # IEEE double-precision floating-point format

type_sizes = {
    INT_8: 1, INT_16: 2, INT_32: 4, INT_64: 8,
    UINT_8: 1, UINT_16: 2, UINT_32: 4, FLOAT_64: 8,
}

message_names = {
    0x01: "INIT",
    0x0E: "ACK_FAILURE",
    0x0F: "RESET",
    0x10: "RUN",
    0x2F: "DISCARD_ALL",
    0x3F: "PULL_ALL",
    0x70: "SUCCESS",
    0x71: "RECORD",
    0x7E: "IGNORED",
    0x7F: "FAILURE",
}


def h(data):
    return ":".join("{:02X}".format(b) for b in bytearray(data))


class Packed(object):

    def __init__(self, data, offset=0):
        self.data = data
        self.offset = offset

    def raw_unpack(self, type_code):
        value, = raw_unpack(type_code, self.data, self.offset)
        self.offset += type_sizes[type_code]
        return value

    def unpack_string(self, size):
        end = self.offset + size
        value = self.data[self.offset:end].decode("utf-8")
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

    def unpack_all(self):
        while self.offset < len(self.data):
            yield next(self.unpack(1))


class Peer(object):

    def __init__(self, socket, address):
        self.socket = socket
        self.address = address


class ProxyPair(Thread):

    def __init__(self, client, server):
        super(ProxyPair, self).__init__()
        self.client = client
        self.server = server
        print("C: [CONNECT] {} -> {}".format(self.client.address, self.server.address))

    def run(self):
        client = self.client
        server = self.server
        print("C: [BOLT] {}".format(h(self.forward(client, server, 4))))
        print("C: [HANDSHAKE] {}".format(h(self.forward(client, server, 16))))
        print("S: [HANDSHAKE] {}".format(h(self.forward(server, client, 4))))
        more = True
        while more:
            try:
                self.forward_exchange(client, server)
            except RuntimeError:
                more = False
        print("C: [CLOSE]")

    def forward(self, source, target, size):
        data = source.socket.recv(size)
        target.socket.sendall(data)
        return data

    def forward_chunk(self, source, target):
        chunk_header = self.forward(source, target, 2)
        if not chunk_header:
            raise RuntimeError()
        chunk_size = chunk_header[0] * 0x100 + chunk_header[1]
        return self.forward(source, target, chunk_size)

    def forward_message(self, source, target):
        d = b""
        size = -1
        while size:
            data = self.forward_chunk(source, target)
            size = len(data)
            d += data
        return d

    def forward_exchange(self, client, server):
        rq_message = self.forward_message(client, server)
        rq_signature = rq_message[1]
        rq_data = Packed(rq_message[2:]).unpack_all()
        print("C: {} {}".format(message_names[rq_signature], " ".join(map(repr, rq_data))))
        more = True
        while more:
            rs_message = self.forward_message(server, client)
            rs_signature = rs_message[1]
            rs_data = Packed(rs_message[2:]).unpack_all()
            print("S: {} {}".format(message_names[rs_signature], " ".join(map(repr, rs_data))))
            more = rs_signature == 0x71


class ProxyServer(Thread):

    running = False

    def __init__(self, bind_address, server_address):
        super(ProxyServer, self).__init__()
        self.socket = socket()
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.bind(bind_address)
        self.socket.listen(0)
        self.server_address = server_address
        self.pairs = []

    def __del__(self):
        self.stop()

    def run(self):
        self.running = True
        while self.running:
            client_socket, client_address = self.socket.accept()
            server_socket = socket()
            server_socket.connect(self.server_address)
            client = Peer(client_socket, client_address)
            server = Peer(server_socket, self.server_address)
            pair = ProxyPair(client, server)
            pair.start()
            self.pairs.append(pair)

    def stop(self):
        self.running = False


def run():
    parser = ArgumentParser(description=HELP, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-b", "--bind-address", help="bind address for the proxy server", default="0.0.0.0:7777")
    parser.add_argument("-s", "--server-address", help="Neo4j server address", default="localhost:7687")

    args = parser.parse_args()
    bind_host, bind_port = args.bind_address.split(":")
    server_host, server_port = args.server_address.split(":")
    server = ProxyServer((bind_host, int(bind_port)), (server_host, int(server_port)))
    server.start()


if __name__ == "__main__":
    run()
