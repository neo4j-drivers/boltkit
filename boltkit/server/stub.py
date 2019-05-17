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
from socket import socket, SOL_SOCKET, SO_REUSEADDR, SHUT_RDWR
from struct import pack as raw_pack, unpack_from as raw_unpack
from threading import Thread

from boltkit.addressing import Address
from boltkit.server.bytetools import h
from boltkit.client import CLIENT, SERVER, BOLT
from boltkit.client.packstream import UINT_16, INT_32, Structure, pack, unpack
from boltkit.server.scripting import Script, ExitCommand


EXIT_OK = 0
EXIT_OFF_SCRIPT = 1
EXIT_TIMEOUT = 2
EXIT_UNKNOWN = 99


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
        self.address = Address(address)
        self.bolt_version = 0


class StubServer(Thread):

    peers = None
    script = Script()

    def __init__(self, script_name=None, listen_addr=None, timeout=None):
        super(StubServer, self).__init__()
        self.address = listen_addr or Address.parse(":17687")
        self.server = socket()
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server.bind((self.address.host, self.address.port_number))
        self.server.listen(0)
        log.info("Listening for incoming connections on «%s»", self.address)
        self.peers = {}
        if script_name:
            self.script = Script(script_name)
        self.running = True
        self.timeout = timeout
        self.exit_code = 0

    def run(self):
        self.peers[self.server] = Peer(self.server, self.address)
        while self.running:
            try:
                read_list, _, _ = select(list(self.peers), [], [], self.timeout)
                if read_list:
                    for sock in read_list:
                        self.read(sock)
                else:
                    log.error("Timed out after waiting %rs for an incoming "
                              "connection", self.timeout)
                    raise SystemExit(EXIT_TIMEOUT)
            except SystemExit as e:
                self.exit_code = e.args[0]
                self.running = False
            except:
                self.exit_code = EXIT_UNKNOWN
                self.running = False
        self.stop()
        log.info("Exiting with code %r", self.exit_code)

    def stop(self):
        if not self.peers:
            return
        peers, self.peers, self.running = list(self.peers.items()), {}, False
        for sock, peer in peers:
            log.debug("~~ <CLOSE> \"%s\" %d", *peer.address)
            try:
                sock.shutdown(SHUT_RDWR)
                sock.close()
            except OSError:
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
        # listen_address = self.peers[sock].address
        serve_address = self.peers[new_sock].address
        log.info("Accepted incoming connection from «%s»", serve_address)

    def handshake(self, sock):
        data = sock.recv(4)
        if data == BOLT:
            log.debug("C: <BOLT>")
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
        log.debug("C: <VERSION> [0x%08x, 0x%08x, 0x%08x, 0x%08x]" % tuple(client_requested_versions))

        v = self.script.bolt_version
        if v not in client_requested_versions:
            raise RuntimeError("Script protocol version %r not offered by client" % v)

        # only single protocol version is currently supported
        response = raw_pack(INT_32, v)
        log.debug("S: <VERSION> 0x%08x" % v)
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
        request = unpack(message_data)

        if self.script.match_request(request):
            # explicitly matched
            log.debug("C: %s", message_repr(v, request))
        elif self.script.match_auto_request(request):
            # auto matched
            log.debug("C! %s", message_repr(v, request))
        else:
            # not matched
            if self.script.lines:
                expected = message_repr(v, self.script.lines[0].message)
            else:
                expected = "END OF SCRIPT"
            log.debug("C: %s", message_repr(v, request))
            log.error("Message mismatch (expected <%s>, "
                      "received <%s>)", expected, message_repr(v, request))
            raise SystemExit(EXIT_OFF_SCRIPT)

        responses = self.script.match_responses()
        if not responses and self.script.match_auto_request(request):
            # These are hard-coded and therefore not very future-proof.
            if request.tag in (CLIENT[v].get("HELLO"), CLIENT[v].get("INIT")):
                responses = [Structure(SERVER[v]["SUCCESS"], {"server": server_agents.get(v, "Neo4j/9.99.999")})]
            elif request.tag == CLIENT[v].get("GOODBYE"):
                log.debug("S: <EXIT>")
                raise SystemExit(EXIT_OK)
            elif request.tag == CLIENT[v]["RUN"]:
                responses = [Structure(SERVER[v]["SUCCESS"], {"fields": []})]
            else:
                responses = [Structure(SERVER[v]["SUCCESS"], {})]
        for response in responses:
            if isinstance(response, Structure):
                data = pack(response)
                self.send_chunk(sock, data)
                self.send_chunk(sock)
                log.debug("S: %s", message_repr(v, Structure(response.tag, *response.fields)))
            elif isinstance(response, ExitCommand):
                raise SystemExit(EXIT_OK)
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
        except OSError:
            log.error("S: <GONE>")
            raise SystemExit(EXIT_OFF_SCRIPT)
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
