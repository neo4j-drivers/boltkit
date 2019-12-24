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
Stub server
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from collections import deque
from itertools import chain
from json import dumps as json_dumps, JSONDecoder
try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from logging import getLogger
from select import select
from socket import socket, SOL_SOCKET, SO_REUSEADDR, SHUT_RDWR
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import exit
from threading import Thread
from uuid import uuid4

from .addressing import Address
from .bytetools import h
from .driver import CLIENT, SERVER, BOLT, MAX_BOLT_VERSION
from .packstream import UINT_16, INT_32, Structure, pack, unpack
from .watcher import watch

TIMEOUT = 30

EXIT_OK = 0
EXIT_OFF_SCRIPT = 1
EXIT_TIMEOUT = 2
EXIT_UNKNOWN = 99


log = getLogger("boltkit.server")

server_agents = {
    1: "Neo4j/3.0.0",
    2: "Neo4j/3.4.0",
    3: "Neo4j/3.5.0",
    4: "Neo4j/4.0.0",
}

default_bolt_version = 2

def message_repr(v, message):
    name = next(key for key, value in chain(CLIENT[v].items(), SERVER[v].items()) if value == message.tag)
    return "%s %s" % (name, " ".join(map(json_dumps, message.fields)))


class Peer(object):

    def __init__(self, socket, address):
        self.socket = socket
        self.address = Address(address)
        self.bolt_version = 0


class Item(object):
    pass


class Line(Item):

    def __init__(self, protocol_version, line_no, peer, message):
        self.protocol_version = protocol_version
        self.line_no = line_no
        self.peer = peer
        self.message = message


class ExitCommand(Item):

    pass


class Script(object):

    def __init__(self, file_name=None):
        self.bolt_version = default_bolt_version
        self.auto = []
        self.lines = deque()
        if file_name:
            self.append(file_name)

    def __nonzero__(self):
        return bool(self.lines)

    def __bool__(self):
        return bool(self.lines)

    def __len__(self):
        return len(self.lines)

    def parse_message(self, message):
        tag, _, data = message.partition(" ")
        v = self.bolt_version
        if tag in CLIENT[v]:
            parsed_tag = CLIENT[v][tag]
        elif tag in SERVER[v]:
            parsed_tag = SERVER[v][tag]
        else:
            raise ValueError("Unknown message type %s" % tag)
        decoder = JSONDecoder()
        parsed = []
        while data:
            data = data.lstrip()
            try:
                decoded, end = decoder.raw_decode(data)
            except ValueError:
                break
            else:
                parsed.append(decoded)
                data = data[end:]
        return Structure(parsed_tag, *parsed)

    def parse_command(self, message):
        tag, _, data = message.partition(" ")
        if tag == "<EXIT>":
            return ExitCommand()
        else:
            raise ValueError("Unknown command %s" % tag)

    def parse_lines(self, lines):
        mode = "C"
        for line_no, line in enumerate(lines, start=1):
            line = line.rstrip()
            if line == "" or line.startswith("//"):
                pass
            elif len(line) >= 2 and line[1] == ":":
                mode = line[0].upper()
                yield line_no, mode, line[2:].lstrip()
            elif mode is not None:
                yield line_no, mode, line.lstrip()

    def append(self, file_name):
        lines = self.lines
        with open(file_name) as f:
            for line_no, mode, line in self.parse_lines(f):
                if mode == "!":
                    command, _, rest = line.partition(" ")
                    if command == "AUTO":
                        self.auto.append(self.parse_message(rest))
                    if command == "BOLT":
                        self.bolt_version = int(rest)
                        if self.bolt_version < 0 or self.bolt_version > MAX_BOLT_VERSION or CLIENT[self.bolt_version] is None:
                            raise RuntimeError("Protocol version %r in script %r is not available "
                                               "in this version of BoltKit" % (self.bolt_version, file_name))
                elif mode in "CS":
                    if line.startswith("<"):
                        lines.append(Line(self.bolt_version, line_no, mode, self.parse_command(line)))
                    else:
                        lines.append(Line(self.bolt_version, line_no, mode, self.parse_message(line)))

    def match_auto_request(self, request):
        for message in self.auto:
            if request.tag == message.tag:
                return True
            elif request == message:
                return True
        return False

    def match_request(self, request):
        if not self.lines:
            return 0
        line = self.lines[0]
        if line.peer != "C":
            return 0
        if match(line.message, request):
            self.lines.popleft()
            return 1
        else:
            return 0

    def match_responses(self):
        responses = []
        while self.lines and self.lines[0].peer == "S":
            line = self.lines.popleft()
            if isinstance(line, Line):
                responses.append(line.message)
            elif isinstance(line, ExitCommand):
                pass
            else:
                raise RuntimeError("Unexpected response %r" % line)
        return responses


def match(expected, actual):
    return expected == actual


class StubServer(Thread):

    peers = None
    script = Script()

    def __init__(self, address, script_name=None, timeout=None):
        super(StubServer, self).__init__()
        self.address = address
        self.server = socket()
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server.bind(self.address)
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
            #except:
            #    self.exit_code = EXIT_UNKNOWN
            #    self.running = False
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
            self.stop()
            raise SystemExit(EXIT_OFF_SCRIPT)

        responses = self.script.match_responses()
        if not responses and self.script.match_auto_request(request):
            # These are hard-coded and therefore not very future-proof.
            if request.tag in (CLIENT[v].get("HELLO"), CLIENT[v].get("INIT")):
                responses = [Structure(SERVER[v]["SUCCESS"], {
                    "connection_id": str(uuid4()),
                    "server": server_agents.get(v, "Neo4j/9.99.999"),
                })]
            elif request.tag == CLIENT[v].get("GOODBYE"):
                log.debug("S: <EXIT>")
                self.stop()
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
                self.stop()
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


def stub():
    parser = ArgumentParser(
        description="Run a stub Bolt server.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  boltstub 9001 example.bolt",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-e", "--enterprise", action="store_true",
                        help="select Neo4j Enterprise Edition (default: Community)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("port", type=int, help="port number to listen for connection on")
    parser.add_argument("script", help="Bolt script file")
    parsed = parser.parse_args()
    watch("boltkit.server")
    server = StubServer(("127.0.0.1", parsed.port), parsed.script)
    server.start()
    try:
        while server.is_alive():
            pass
    except KeyboardInterrupt:
        pass
    exit(0 if not server.script else 1)


if __name__ == "__main__":
    stub()
