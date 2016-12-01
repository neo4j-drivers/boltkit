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
from socket import socket, SOL_SOCKET, SO_REUSEADDR, error as socket_error, SHUT_RDWR
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import exit
from threading import Thread

from .driver import h, UINT_16, CLIENT, SERVER, packed, unpacked, BOLT, BOLT_VERSION
from.watcher import watch


TIMEOUT = 30


log = getLogger("boltkit.server")


def message_repr(tag, *data):
    name = next(key for key, value in chain(CLIENT.items(), SERVER.items()) if value == tag)
    return "%s %s" % (name, " ".join(map(json_dumps, data)))


class Peer(object):

    def __init__(self, address):
        self.address = address
        self.version = 0


class Item(object):
    pass


class Line(Item):

    def __init__(self, line_no, peer, message):
        self.line_no = line_no
        self.peer = peer
        self.message = message

    def __repr__(self):
        return "%s: %s" % (self.peer, message_repr(*self.message))


class ExitCommand(Item):

    pass


class Script(object):

    def __init__(self, file_name=None):
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
        if tag in CLIENT:
            parsed = (CLIENT[tag],)
        elif tag in SERVER:
            parsed = (SERVER[tag],)
        else:
            raise ValueError("Unknown message type %s" % tag)
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
                elif mode in "CS":
                    if line.startswith("<"):
                        lines.append(Line(line_no, mode, self.parse_command(line)))
                    else:
                        lines.append(Line(line_no, mode, self.parse_message(line)))

    def match_auto_request(self, request):
        for message in self.auto:
            if len(message) == 1 and request[0] == message[0]:
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
        self.peers = {}
        if script_name:
            self.script = Script(script_name)
        self.running = True
        self.timeout = timeout or TIMEOUT

    def __del__(self):
        self.stop()

    def run(self):
        self.peers[self.server] = Peer(self.address)
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
            sock.shutdown(SHUT_RDWR)
            sock.close()

    def read(self, sock):
        try:
            if sock == self.server:
                self.accept(sock)
            elif self.peers[sock].version:
                self.handle_request(sock)
            else:
                self.handshake(sock)
        except (KeyError, OSError):
            if self.running:
                raise

    def accept(self, sock):
        new_sock, address = sock.accept()
        self.peers[new_sock] = Peer(address)
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
        # TODO: proper version negotiation
        log.info("C: <VERSION> %s" % h(raw_data))
        response = raw_data[0:4]
        log.info("S: <VERSION> %d" % BOLT_VERSION)
        sock.send(response)
        self.peers[sock].version = 1

    def handle_request(self, sock):
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
            log.info("C: %s", message_repr(*request))
        elif self.script.match_auto_request(request):
            # auto matched
            log.info("C! %s", message_repr(*request))
        else:
            # not matched
            log.error("C: %s", message_repr(*request))

        responses = self.script.match_responses()
        if not responses and self.script.match_auto_request(request):
            responses = [(SERVER["SUCCESS"], {u"fields": []}
                         if request[0] == CLIENT["RUN"] else {})]
        for response in responses:
            if isinstance(response, tuple):
                data = packed(response)
                self.send_chunk(sock, data)
                self.send_chunk(sock)
                log.info("S: %s", message_repr(*response))
            elif isinstance(response, ExitCommand):
                exit(0)
            else:
                raise RuntimeError("Unknown response type %r" % response)

    def send_chunk(self, sock, data=b""):
        header = raw_pack(UINT_16, len(data))
        header_hex = self.send_bytes(sock, header)
        data_hex = self.send_bytes(sock, data)
        return "[%s] %s" % (header_hex, data_hex)

    def send_bytes(self, sock, data):
        try:
            sock.sendall(data)
        except socket_error as error:
            log.error("S: <GONE>")
            exit(1)
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
