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


from itertools import cycle
from logging import getLogger
from socket import socket, SOL_SOCKET, SO_REUSEADDR, AF_INET, AF_INET6
from struct import unpack_from as raw_unpack
from threading import Thread

from boltkit.addressing import Address, AddressList
from boltkit.server.bytetools import h
from boltkit.client import CLIENT, SERVER
from boltkit.client.packstream import UINT_64, UINT_32, UINT_16, Unpackable


log = getLogger("boltkit")


class Peer(object):
    def __init__(self, socket, address, session_idx):
        self.socket = socket
        self.bs_cache = b''
        self.address = address
        self.bolt_version = 0
        self.session_idx = 0


class ProxyPair(Thread):

    def __init__(self, client, server, is_websocket=False):
        super(ProxyPair, self).__init__()
        self.client = client
        self.server = server
        self.is_websocket = is_websocket

    def get_version(self):
        client, server = self.client, self.server
        log.debug("C[{}]: <CONNECT> {} -> {}".format(self.client.session_idx, self.client.address, self.server.address))
        log.debug("C[{}]: <BOLT> {}".format(self.client.session_idx, h(self.forward_bytes(self.client, self.server, 4, is_websocket=self.is_websocket))))
        log.debug("C[{}]: <VERSION> {}".format(self.client.session_idx, h(self.forward_bytes(self.client, self.server, 16, is_websocket=self.is_websocket))))
        raw_bolt_version = self.forward_bytes(server, client, 4, is_websocket=self.is_websocket)
        bolt_version, = raw_unpack(UINT_32, raw_bolt_version)
        bolt_version = (bolt_version % 0x100, bolt_version // 0x100)
        self.client.bolt_version = self.server.bolt_version = bolt_version
        log.debug("S[{}]: <VERSION> {}".format(self.server.session_idx, h(bolt_version)))
        self.client_messages = {v: k for k, v in CLIENT[self.client.bolt_version].items()}
        self.server_messages = {v: k for k, v in SERVER[self.server.bolt_version].items()}

    def read_header(self, client, server, TAG):
        while True:
            line = self.forward_line(client, server)
            log.debug("{} Header: {}".format(TAG, line))
            if line == b'\r\n':
                break

    def run(self):
        if self.is_websocket:
            self.read_header(self.client, self.server, "Request[{}]".format(self.client.session_idx))
            self.read_header(self.server, self.client, "Response[{}]".format(self.server.session_idx))
        self.get_version()
        client = self.client
        server = self.server
        more = True
        while more:
            try:
                self.forward_exchange(client, server, is_websocket=self.is_websocket)
            except RuntimeError:
                more = False
        log.debug("C[{}]: <CLOSE>".format(self.client.session_idx))

    @classmethod
    def unmask(cls, mask, bs):
        bytes_unmask = []
        for m, byte in zip(cycle(mask), bs):
            bytes_unmask.append(m ^ byte)
        return bytes(bytes_unmask)

    @classmethod
    def source_recv_then_forward_target(cls, source, target, size, is_websocket=False):
        if is_websocket:
            while len(source.bs_cache) < size:
                source.bs_cache += cls.decompress_bytes_from_websocket(source, target)
            data = source.bs_cache[:size]
            source.bs_cache = source.bs_cache[size:]
            return data
        else:
            data = source.socket.recv(size)
            target.socket.sendall(data)
        return data

    @classmethod
    def decompress_bytes_from_websocket(cls, source, target):
        '''
        Websocket Protocol Defines: 
        Ref: https://tools.ietf.org/html/rfc6455
     0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    +-+-+-+-+-------+-+-------------+-------------------------------+
    |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
    |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
    |N|V|V|V|       |S|             |   (if payload len==126/127)   |
    | |1|2|3|       |K|             |                               |
    +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
    |     Extended payload length continued, if payload len == 127  |
    + - - - - - - - - - - - - - - - +-------------------------------+
    |                               |Masking-key, if MASK set to 1  |
    +-------------------------------+-------------------------------+
    | Masking-key (continued)       |          Payload Data         |
    +-------------------------------- - - - - - - - - - - - - - - - +
    :                     Payload Data continued ...                :
    + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
    |                     Payload Data continued ...                |
    +---------------------------------------------------------------+
        '''
        opthead = cls.source_recv_then_forward_target(source, target, 2)
        if len(opthead) < 2:
            raise RuntimeError("RECV Empty: May caused by socket loss!")
        # FIN = opthead[0] >> 7
        # RSV1 = opthead[0] >> 6 & 1
        # RSV2 = opthead[0] >> 5 & 1
        # RSV3 = opthead[0] >> 4 & 1
        # OPCODE = opthead[0] & 0b1111
        MASK = opthead[1] >> 7
        PAYLOAD_LEN = opthead[1] & 0b01111111
        if PAYLOAD_LEN == 126:
            PAYLOAD_LEN = raw_unpack(UINT_16, cls.source_recv_then_forward_target(source, target, 2))[0]
        elif PAYLOAD_LEN == 127:
            PAYLOAD_LEN = raw_unpack(UINT_64, cls.source_recv_then_forward_target(source, target, 8))[0]
        if MASK == 0:
            data = cls.source_recv_then_forward_target(source, target, PAYLOAD_LEN)
            return data
        else:
            mask = cls.source_recv_then_forward_target(source, target, 4)
            mask_data = cls.source_recv_then_forward_target(source, target, PAYLOAD_LEN)
            unmask_data = cls.unmask(mask, mask_data)
            return unmask_data

    @classmethod
    def forward_bytes(cls, source, target, size, is_websocket=False):
        return cls.source_recv_then_forward_target(source, target, size, is_websocket=is_websocket)


    @classmethod
    def forward_line(cls, source, target):
        data = b''
        while True:
            data += cls.source_recv_then_forward_target(source, target, 1)  # source.socket.recv(1)
            if len(data) >= 2 and data[-2:] == b'\r\n':
                return data

    @classmethod
    def forward_chunk(cls, source, target, is_websocket=False):
        chunk_header = cls.forward_bytes(source, target, 2, is_websocket)
        if not chunk_header:
            raise RuntimeError()
        chunk_size = chunk_header[0] * 0x100 + chunk_header[1]
        return cls.forward_bytes(source, target, chunk_size, is_websocket)

    @classmethod
    def forward_message(cls, source, target, is_websocket=False):
        d = b""
        size = -1
        while size:
            data = cls.forward_chunk(source, target, is_websocket)
            size = len(data)
            d += data
        return d

    def forward_exchange(self, client, server, is_websocket=False):
        rq_message = self.forward_message(client, server, is_websocket)
        rq_signature = rq_message[1]
        rq_data = Unpackable(rq_message[2:]).unpack_all()
        log.debug("C[{}]: {} {}".format(self.client.session_idx, self.client_messages[rq_signature], " ".join(map(repr, rq_data))))
        more = True
        while more:
            rs_message = self.forward_message(server, client, is_websocket)
            rs_signature = rs_message[1]
            rs_data = Unpackable(rs_message[2:]).unpack_all()
            log.debug("S[{}]: {} {}".format(self.server.session_idx, self.server_messages[rs_signature], " ".join(map(repr, rs_data))))
            more = rs_signature == 0x71


class ProxyServer(Thread):

    running = False

    def __init__(self, server_addr, listen_addr=None, is_websocket=False):
        super(ProxyServer, self).__init__()
        self.socket = socket()
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        addresses = AddressList([listen_addr or Address.parse(":17687")])
        addresses.resolve(family=AF_INET)
        self.socket.bind(addresses[0])
        self.socket.listen(0)
        server_addr.resolve()
        self.server_addr = server_addr[0]
        self.pairs = []
        self.is_websocket = is_websocket

    def __del__(self):
        self.stop()

    def run(self):
        self.running = True
        session_idx = 0
        while self.running:
            session_idx += 1
            client_socket, client_address = self.socket.accept()
            server_socket = socket({2: AF_INET, 4: AF_INET6}[len(self.server_addr)])
            server_socket.connect(self.server_addr)
            client = Peer(client_socket, client_address, session_idx)
            server = Peer(server_socket, self.server_addr, session_idx)
            pair = ProxyPair(client, server, self.is_websocket)
            pair.start()
            self.pairs.append(pair)

    def stop(self):
        self.running = False
