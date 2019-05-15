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


from collections import deque
from json import JSONDecoder

from boltkit.client import CLIENT, SERVER, MAX_BOLT_VERSION, Structure


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
        self.bolt_version = 1
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
