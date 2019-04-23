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

from itertools import chain
from json import dumps as json_dumps

from logging import getLogger
from select import select
from socket import socket, SOL_SOCKET, SO_REUSEADDR, error as socket_error, SHUT_RDWR, AF_INET, AF_INET6
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import exit
from threading import Thread

from boltkit.bytetools import h
from boltkit.packstream import UINT_16, UINT_32, INT_32, Packed, Structure, packed, unpacked
from boltkit.client import CLIENT, SERVER, BOLT
from boltkit.scripting import Script, ExitCommand


TIMEOUT = 30


log = getLogger("boltkit")

server_agents = {
    1: "Neo4j/3.0.0",
    2: "Neo4j/3.4.0",
    3: "Neo4j/3.5.0",
    4: "Neo4j/4.0.0",
}


def message_repr(v, message):
    name = next(key for key, value in chain(CLIENT[v].items(), SERVER[v].items()) if value == message.tag)
    return "%s %s" % (name, " ".join(map(json_dumps, message.fields)))


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

    def forward_bytes(self, source, target, size):
        data = source.socket.recv(size)
        target.socket.sendall(data)
        return data

    def forward_chunk(self, source, target):
        chunk_header = self.forward_bytes(source, target, 2)
        if not chunk_header:
            raise RuntimeError()
        chunk_size = chunk_header[0] * 0x100 + chunk_header[1]
        return self.forward_bytes(source, target, chunk_size)

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
        log.debug("C: {} {}".format(self.client_messages[rq_signature], " ".join(map(repr, rq_data))))
        more = True
        while more:
            rs_message = self.forward_message(server, client)
            rs_signature = rs_message[1]
            rs_data = Packed(rs_message[2:]).unpack_all()
            log.debug("S: {} {}".format(self.server_messages[rs_signature], " ".join(map(repr, rs_data))))
            more = rs_signature == 0x71


class ProxyServer(Thread):

    running = False

    def __init__(self, bind_address, server_addr):
        super(ProxyServer, self).__init__()
        self.socket = socket()
        self.socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.socket.bind(bind_address)
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


class StubServer(Thread):

    peers = None
    script = Script()

    def __init__(self, address, script_name=None, timeout=None, user=None, password=None):
        super(StubServer, self).__init__()
        self.address = address
        self.settings = {
            "user": user,
            "password": password,
        }
        self.server = socket()
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server.bind(self.address)
        self.server.listen(0)
        self.peers = {}
        if script_name:
            self.script = Script(script_name)
        self.running = True
        self.timeout = timeout or TIMEOUT

    def __del__(self):
        self.stop()

    def run(self):
        self.peers[self.server] = Peer(self.server, self.address)
        while self.running:
            read_list, _, _ = select(list(self.peers), [], [], self.timeout)
            if read_list:
                for sock in read_list:
                    self.read(sock)
            else:
                log.error("C: <TIMEOUT> %ds", self.timeout)
                exit(1)

    def stop(self):
        if not self.peers:
            return
        peers, self.peers, self.running = list(self.peers.items()), {}, False
        for sock, peer in peers:
            log.info("~~ <CLOSE> \"%s\" %d", *peer.address)
            try:
                sock.shutdown(SHUT_RDWR)
                sock.close()
            except socket_error:
                pass

    def read(self, sock):
        try:
            if sock == self.server:
                self.accept(sock)
            elif self.peers[sock].bolt_version:
                self.handle_request(sock)
            else:
                self.handshake(sock)
        except (KeyError, OSError):
            if self.running:
                raise

    def accept(self, sock):
        new_sock, address = sock.accept()
        self.peers[new_sock] = Peer(new_sock, address)
        listen_address = self.peers[sock].address
        serve_address = self.peers[new_sock].address
        log.info("~~ <ACCEPT> \"%s\" %d -> %d", listen_address[0], listen_address[1], serve_address[1])

    def handshake(self, sock):
        data = sock.recv(4)
        if data == BOLT:
            log.info("C: <BOLT>")
        else:
            if data:
                log.error("C: <#?@!>")
            self.stop()
            return
        raw_data = sock.recv(16)
        suggested_version_1, = raw_unpack(INT_32, raw_data, 0)
        suggested_version_2, = raw_unpack(INT_32, raw_data, 4)
        suggested_version_3, = raw_unpack(INT_32, raw_data, 8)
        suggested_version_4, = raw_unpack(INT_32, raw_data, 12)
        client_requested_versions = [suggested_version_1, suggested_version_2, suggested_version_3, suggested_version_4]
        log.info("C: <VERSION> [0x%08x, 0x%08x, 0x%08x, 0x%08x]" % tuple(client_requested_versions))

        v = self.script.bolt_version
        if v not in client_requested_versions:
            raise RuntimeError("Script protocol version %r not offered by client" % v)

        # only single protocol version is currently supported
        response = raw_pack(INT_32, v)
        log.info("S: <VERSION> 0x%08x" % v)
        self.peers[sock].bolt_version = v
        sock.send(response)

    def handle_request(self, sock):
        v = self.peers[sock].bolt_version

        chunked_data = b""
        message_data = b""
        chunk_size = -1
        debug = []
        while chunk_size != 0:
            chunk_header = sock.recv(2)
            if len(chunk_header) == 0:
                self.stop()
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
        request = unpacked(message_data)

        if self.script.match_request(request):
            # explicitly matched
            log.info("C: %s", message_repr(v, request))
        elif self.script.match_auto_request(request):
            # auto matched
            log.info("C! %s", message_repr(v, request))
        else:
            # not matched
            log.error("C: %s", message_repr(v, request))

        responses = self.script.match_responses()
        if not responses and self.script.match_auto_request(request):
            # These are hard-coded and therefore not very future-proof.
            if request.tag in (CLIENT[v].get("HELLO"), CLIENT[v].get("INIT")):
                responses = [Structure(SERVER[v]["SUCCESS"], {"server": server_agents.get(v, "Neo4j/9.99.999")})]
            elif request.tag == CLIENT[v].get("GOODBYE"):
                log.info("S: <EXIT>")
                exit(0)
            elif request.tag == CLIENT[v]["RUN"]:
                responses = [Structure(SERVER[v]["SUCCESS"], {"fields": []})]
            else:
                responses = [Structure(SERVER[v]["SUCCESS"], {})]
        for response in responses:
            if isinstance(response, Structure):
                data = packed(response)
                self.send_chunk(sock, data)
                self.send_chunk(sock)
                log.info("S: %s", message_repr(v, Structure(response.tag, *response.fields)))
            elif isinstance(response, ExitCommand):
                exit(0)
            else:
                raise RuntimeError("Unknown response type %r" % (response,))

    def send_chunk(self, sock, data=b""):
        header = raw_pack(UINT_16, len(data))
        header_hex = self.send_bytes(sock, header)
        data_hex = self.send_bytes(sock, data)
        return "[%s] %s" % (header_hex, data_hex)

    def send_bytes(self, sock, data):
        try:
            sock.sendall(data)
        except socket_error:
            log.error("S: <GONE>")
            exit(1)
        else:
            return h(data)


def stub_test(script, port=17687):
    """ Decorator for stub tests.
    """
    def f__(f):
        def f_(*args, **kwargs):
            server = StubServer(("127.0.0.1", port), script, timeout=5)
            server.start()
            kwargs["server"] = server
            yield f(*args, **kwargs)
            server.stop()
        f_.__name__ = f.__name__
        f_.__doc__ = f.__doc__
        f_.__dict__.update(f.__dict__)
        return f_
    return f__
