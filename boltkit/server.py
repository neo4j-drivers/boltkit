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
from json import loads as json_loads, JSONDecoder, JSONDecodeError
from select import select
from socket import socket, SOL_SOCKET, SO_REUSEADDR
from struct import pack as raw_pack, unpack_from as raw_unpack
from sys import argv, stderr, exit
from threading import Thread

from .driver import h, UINT_16, BOLT, pack, unpack, message_repr


def parse_lines(lines):
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


def parse_script(lines):
    for line_no, mode, line in parse_lines(lines):
        print(line_no, mode, parse_message(line))


class Server:

    def __init__(self, address):
        self.address = address
        self.version = 0


class StubServer(Thread):

    def __init__(self, address, responses=None):
        super(StubServer, self).__init__()
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


class StubCluster:

    def __init__(self, specs):
        self.specs = specs
        self.servers = []
        for spec in self.specs:
            bind_address = ("127.0.0.1", spec.port)
            server = StubServer(bind_address, spec.responses)
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
    parsed = (BOLT.get(tag, -1),)
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
    cluster = StubCluster(specs)
    cluster.start()
    try:
        while cluster.is_alive():
            pass
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
