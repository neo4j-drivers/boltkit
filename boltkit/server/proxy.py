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
Server components, including stub server and proxy server.
"""


from logging import getLogger
from socket import socket, SOL_SOCKET, SO_REUSEADDR, AF_INET, AF_INET6
from struct import unpack_from as raw_unpack
from threading import Thread

from boltkit.addressing import Address
from boltkit.server.bytetools import h
from boltkit.client import CLIENT, SERVER
from boltkit.client.packstream import UINT_32, Unpackable


log = getLogger("boltkit")


class Peer(object):

    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.bolt_version = 0


class ProxyPair(Thread):

    def __init__(self, client, server):
        super(ProxyPair, self).__init__()
        self.client = client
        self.server = server
        log.debug("C: <CONNECT> {} -> {}".format(self.client.address, self.server.address))
        log.debug("C: <BOLT> {}".format(h(self.forward_bytes(client, server, 4))))
        log.debug("C: <VERSION> {}".format(h(self.forward_bytes(client, server, 16))))
        raw_bolt_version = self.forward_bytes(server, client, 4)
        bolt_version, = raw_unpack(UINT_32, raw_bolt_version)
        self.client.bolt_version = self.server.bolt_version = bolt_version
        log.debug("S: <VERSION> {}".format(h(raw_bolt_version)))
        self.client_messages = {v: k for k, v in CLIENT[self.client.bolt_version].items()}
        self.server_messages = {v: k for k, v in SERVER[self.server.bolt_version].items()}

    def run(self):
        client = self.client
        server = self.server
        more = True
        while more:
            try:
                self.forward_exchange(client, server)
            except RuntimeError:
                more = False
        log.debug("C: <CLOSE>")

    @classmethod
    def forward_bytes(cls, source, target, size):
        data = source.socket.recv(size)
        target.socket.sendall(data)
        return data

    @classmethod
    def forward_chunk(cls, source, target):
        chunk_header = cls.forward_bytes(source, target, 2)
        if not chunk_header:
            raise RuntimeError()
        chunk_size = chunk_header[0] * 0x100 + chunk_header[1]
        return cls.forward_bytes(source, target, chunk_size)

    @classmethod
    def forward_message(cls, source, target):
        d = b""
        size = -1
        while size:
            data = cls.forward_chunk(source, target)
            size = len(data)
            d += data
        return d

    def forward_exchange(self, client, server):
        rq_message = self.forward_message(client, server)
        rq_signature = rq_message[1]
        rq_data = Unpackable(rq_message[2:]).unpack_all()
        log.debug("C: {} {}".format(self.client_messages[rq_signature], " ".join(map(repr, rq_data))))
        more = True
        while more:
            rs_message = self.forward_message(server, client)
            rs_signature = rs_message[1]
            rs_data = Unpackable(rs_message[2:]).unpack_all()
            log.debug("S: {} {}".format(self.server_messages[rs_signature], " ".join(map(repr, rs_data))))
            more = rs_signature == 0x71


class ProxyServer(Thread):

    running = False

    def __init__(self, server_addr, listen_addr=None):
        super(ProxyServer, self).__init__()
        self.socket = socket()
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.bind(listen_addr or Address.parse(":17687"))
        self.socket.listen(0)
        server_addr.resolve()
        self.server_addr = server_addr[0]
        self.pairs = []

    def __del__(self):
        self.stop()

    def run(self):
        self.running = True
        while self.running:
            client_socket, client_address = self.socket.accept()
            server_socket = socket({2: AF_INET, 4: AF_INET6}[len(self.server_addr)])
            server_socket.connect(self.server_addr)
            client = Peer(client_socket, client_address)
            server = Peer(server_socket, self.server_addr)
            pair = ProxyPair(client, server)
            pair.start()
            self.pairs.append(pair)

    def stop(self):
        self.running = False
