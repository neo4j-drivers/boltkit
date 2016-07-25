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

from collections import deque
from itertools import chain
from json import dumps as json_dumps, JSONDecoder
try:
    from json import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError
from os.path import basename
from select import select
from socket import socket, SOL_SOCKET, SO_REUSEADDR
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import argv, exit
from threading import Thread

from .driver import h, UINT_16, CLIENT, SERVER, packed, unpacked, BOLT, BOLT_VERSION
from .watcher import red, green, blue


TIMEOUT = 30


def message_repr(tag, *data):
    name = next(key for key, value in chain(CLIENT.items(), SERVER.items()) if value == tag)
    return "%s %s" % (name, " ".join(map(json_dumps, data)))


def write(text, *args, **kwargs):
    colour = kwargs.get("colour")
    if colour:
        print(colour(text % args))
    else:
        print(text % args)


class Peer(object):

    def __init__(self, address):
        self.address = address
        self.version = 0


class StubServer(Thread):

    def __init__(self, address, script):
        super(StubServer, self).__init__()
        self.server = socket()
        self.server.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server.bind(address)
        self.server.listen(0)
        self.peers = {self.server: Peer(address)}
        self.script = script
        self.running = True

    def run(self):
        while self.running:
            read_list, _, _ = select(list(self.peers), [], [], TIMEOUT)
            if read_list:
                for sock in read_list:
                    self.read(sock)
            else:
                write("C: <TIMEOUT> %ds" % TIMEOUT, colour=red)
                exit(1)

    def read(self, sock):
        if sock == self.server:
            self.accept(sock)
        elif self.peers[sock].version:
            self.handle_request(sock)
        else:
            self.handshake(sock)

    def close(self, sock):
        write("~~ <CLOSE> \"%s\" %d", *self.peers[sock].address)
        del self.peers[sock]
        sock.close()
        self.running = False

    def accept(self, sock):
        new_sock, address = sock.accept()
        self.peers[new_sock] = Peer(address)
        listen_address = self.peers[sock].address
        serve_address = self.peers[new_sock].address
        write("~~ <ACCEPT> \"%s\" %d -> %d", listen_address[0], listen_address[1], serve_address[1])

    def handshake(self, sock):
        data = sock.recv(4)
        if data == BOLT:
            write("C: <BOLT>")
        else:
            write("C: <#?@!>")
            self.close(sock)
            return
        raw_data = sock.recv(16)
        # TODO: proper version negotiation
        write("C: <VERSION> %s" % h(raw_data))
        response = raw_data[0:4]
        write("S: <VERSION> %d" % BOLT_VERSION)
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
                self.close(sock)
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
            write("C: %s", message_repr(*request), colour=green)
        elif self.script.match_auto_request(request):
            # auto matched
            write("C: %s", message_repr(*request), colour=blue)
        else:
            # not matched
            write("C: %s", message_repr(*request), colour=red)

        responses = self.script.match_responses()
        colour = green
        if not responses and self.script.match_auto_request(request):
            responses = [(SERVER["SUCCESS"], {u"fields": []}
                         if request[0] == CLIENT["RUN"] else {})]
            colour = blue
        for response in responses:
            data = packed(response)
            self.send_chunk(sock, data)
            self.send_chunk(sock)
            write("S: %s", message_repr(*response), colour=colour)

    def send_chunk(self, sock, data=b""):
        header = raw_pack(UINT_16, len(data))
        sock.send(header)
        return "[%s] %s" % (h(header), self.send_bytes(sock, data))

    def send_bytes(self, sock, data):
        sock.send(data)
        return h(data)


class StubCluster(object):

    def __init__(self, specs):
        self.specs = specs
        self.servers = []
        for spec in self.specs:
            bind_address = ("127.0.0.1", spec.port)
            server = StubServer(bind_address, spec.script)
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

    def scripts_consumed(self):
        for server in self.servers:
            if server.script:
                return False
        return True


class ServerSpec(object):

    def __init__(self, port, script):
        self.port = port
        self.script = script


class Line(object):

    def __init__(self, line_no, peer, message):
        self.line_no = line_no
        self.peer = peer
        self.message = message

    def __repr__(self):
        return "%s: %s" % (self.peer, message_repr(*self.message))


class Script(object):

    def __init__(self):
        self.auto = []
        self.lines = deque()

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
            responses.append(self.lines.popleft().message)
        return responses


def match(expected, actual):
    return expected == actual


def stub():
    if len(argv) < 2:
        print("usage: %s <ports> <script> [<script> ...]" % basename(argv[0]))
        exit()
    specs = []
    for i, port_string in enumerate(argv[1].split(":"), start=2):
        script = Script()
        try:
            script_name = argv[i]
        except IndexError:
            pass
        else:
            script.append(script_name)
        spec = ServerSpec(int(port_string), script)
        specs.append(spec)
    cluster = StubCluster(specs)
    cluster.start()
    try:
        while cluster.is_alive():
            pass
    except KeyboardInterrupt:
        pass
    exit(0 if cluster.scripts_consumed() else 1)


if __name__ == "__main__":
    stub()
