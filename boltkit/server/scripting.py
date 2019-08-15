#!/usr/bin/env python
# coding: utf-8

# Copyright (c) 2002-2019 "Neo Technology,"
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


from asyncio import sleep, IncompleteReadError
from json import JSONDecoder
from textwrap import wrap

from boltkit.packstream import Structure


def splart(s):
    parts = s.split(maxsplit=1)
    while len(parts) < 2:
        parts.append("")
    return parts


class BoltScript:

    protocol_version = ()

    messages = {
        "C": {},
        "S": {},
    }

    def __new__(cls, *lines, auto=None, filename=None, handshake_data=None,
                port=None, version=None):
        if version is None or version in {(1,), (3, 0), (3, 1), (3, 2), (3, 3)}:
            return super().__new__(Bolt1Script)
        elif version in {(2,), (3, 4)}:
            return super().__new__(Bolt2Script)
        elif version in {(3,), (3, 5), (3, 6)}:
            return super().__new__(Bolt3Script)
        elif version in {(4,), (4, 0)}:
            return super().__new__(Bolt4Script)
        else:
            raise BoltScriptError("Unsupported version {}".format(version))

    def __init__(self, *lines, auto=None, filename=None, handshake_data=None,
                 port=None, **_):
        self._lines = []
        for line in lines:
            self.append(line)
        self._auto = list(auto or [])
        self.filename = filename or ""
        self.handshake_data = handshake_data
        self.port = port or 0

    def __iter__(self):
        for line in self._lines:
            yield line

    def append(self, line):
        line.script = self
        self._lines.append(line)

    def auto_match(self, tag):
        return self.tag_name("C", tag) in self._auto

    def on_auto_match(self, request):
        raise NotImplementedError

    @classmethod
    def tag(cls, role, name):
        tags = [k for k, v in cls.messages[role].items() if v == name]
        if tags:
            return tags[0]
        else:
            raise ValueError("Message %r not available for protocol "
                             "version %s" % (name, ".".join(map(str, cls.protocol_version))))

    @classmethod
    def tag_name(cls, role, tag):
        try:
            return cls.messages[role][tag]
        except KeyError:
            return "<Structure[0x%02X]>" % ord(tag)

    @classmethod
    def parse(cls, source):
        return cls.parse_lines(source.splitlines())

    @classmethod
    def load(cls, filename):
        with open(filename) as fin:
            def iter_lines():
                for line in fin:
                    yield line

            script = cls.parse_lines(iter_lines())
            script.filename = filename
            return script

    @classmethod
    def parse_lines(cls, lines):
        out = []
        metadata = {
            "auto": [],
        }
        last_role = ""
        for line_no, line in enumerate(lines, start=1):
            role, tag, fields = cls.parse_line(line)
            if not tag:
                continue
            if role:
                last_role = role
            else:
                role = last_role
            if role == "!":
                if tag == "AUTO":
                    metadata["auto"].append(fields[0])
                elif tag in {"BOLT", "NEO4J"}:
                    metadata["version"] = tuple(map(int, str(fields[0]).split(".")))
                elif tag == "HANDSHAKE":
                    data = bytearray(int(_, 16) for _ in wrap("".join(map(str, fields)), 2))
                    metadata["handshake_data"] = data
                elif tag == "PORT":
                    metadata["port"] = fields[0]
                else:
                    raise ValueError("Unknown meta tag {!r}".format(tag))
                pass
            elif role == "C":
                out.append(ClientMessageLine(tag, *fields))
                out[-1].line_no = line_no
            elif role == "S":
                if tag.startswith("<") and tag.endswith(">"):
                    if tag == "<EXIT>":
                        out.append(ServerExitLine())
                        out[-1].line_no = line_no
                    elif tag == "<RAW>":
                        data = bytearray(int(_, 16) for _ in wrap("".join(map(str, fields)), 2))
                        out.append(ServerRawBytesLine(data))
                        out[-1].line_no = line_no
                    elif tag == "<SLEEP>":
                        out.append(ServerSleepLine(fields[0]))
                        out[-1].line_no = line_no
                    else:
                        raise ValueError("Unknown command %r" % (tag,))
                else:
                    out.append(ServerMessageLine(tag, *fields))
                    out[-1].line_no = line_no
            else:
                raise ValueError("Unknown role %r" % (role,))
        return BoltScript(*out, **metadata)

    @classmethod
    def parse_line(cls, line):
        role = ""
        tag, data = splart(line.strip())
        fields = []
        if tag.endswith(":"):
            role = tag.rstrip(":")
            tag, data = splart(data)
        decoder = JSONDecoder()
        while data:
            data = data.lstrip()
            try:
                decoded, end = decoder.raw_decode(data)
            except ValueError:
                fields.append(data)
                data = ""
            else:
                fields.append(decoded)
                data = data[end:]
        return role, tag, fields


class Bolt1Script(BoltScript):

    protocol_version = (1, 0)

    messages = {
        "C": {
            b"\x01": "INIT",
            b"\x0E": "ACK_FAILURE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x2F": "DISCARD_ALL",
            b"\x3F": "PULL_ALL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/3.3.0"

    def on_handshake(self, request):
        handshake_data = self.handshake_data
        if handshake_data is None:
            handshake_data = bytearray()
            for value in self.protocol_version:
                handshake_data.insert(0, value)
            while len(handshake_data) < 4:
                handshake_data.insert(0, 0)
        return bytes(handshake_data)

    def on_auto_match(self, request):
        if request.tag == b"\x01":
            yield Structure(b"\x70", {
                "server": self.server_agent,
            })
        else:
            yield Structure(b"\x70", {})


class Bolt2Script(BoltScript):

    protocol_version = (2, 0)

    messages = {
        "C": {
            b"\x01": "INIT",
            b"\x0E": "ACK_FAILURE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x2F": "DISCARD_ALL",
            b"\x3F": "PULL_ALL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/3.4.0"

    def on_handshake(self, request):
        handshake_data = self.handshake_data
        if handshake_data is None:
            handshake_data = bytearray()
            for value in self.protocol_version:
                handshake_data.insert(0, value)
            while len(handshake_data) < 4:
                handshake_data.insert(0, 0)
        return bytes(handshake_data)

    def on_auto_match(self, request):
        if request.tag == b"\x01":
            yield Structure(b"\x70", {
                "server": self.server_agent,
            })
        else:
            yield Structure(b"\x70", {})


class Bolt3Script(BoltScript):

    protocol_version = (3, 0)

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD_ALL",
            b"\x3F": "PULL_ALL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/3.5.0"

    def on_handshake(self, request):
        handshake_data = self.handshake_data
        if handshake_data is None:
            handshake_data = bytearray()
            for value in self.protocol_version:
                handshake_data.insert(0, value)
            while len(handshake_data) < 4:
                handshake_data.insert(0, 0)
        return bytes(handshake_data)

    def on_auto_match(self, request):
        if request.tag == b"\x01":
            yield Structure(b"\x70", {
                "connection_id": "bolt-0",
                "server": self.server_agent,
            })
        else:
            yield Structure(b"\x70", {})


class Bolt4Script(BoltScript):

    protocol_version = (4, 0)

    messages = {
        "C": {
            b"\x01": "HELLO",
            b"\x02": "GOODBYE",
            b"\x0F": "RESET",
            b"\x10": "RUN",
            b"\x11": "BEGIN",
            b"\x12": "COMMIT",
            b"\x13": "ROLLBACK",
            b"\x2F": "DISCARD",
            b"\x3F": "PULL",
        },
        "S": {
            b"\x70": "SUCCESS",
            b"\x71": "RECORD",
            b"\x7E": "IGNORED",
            b"\x7F": "FAILURE",
        },
    }

    server_agent = "Neo4j/4.0.0"

    def on_handshake(self, request):
        handshake_data = self.handshake_data
        if handshake_data is None:
            handshake_data = bytearray()
            for value in self.protocol_version:
                handshake_data.insert(0, value)
            while len(handshake_data) < 4:
                handshake_data.insert(0, 0)
        return bytes(handshake_data)

    def on_auto_match(self, request):
        if request.tag == b"\x01":
            yield Structure(b"\x70", {
                "connection_id": "bolt-0",
                "server": self.server_agent,
            })
        else:
            yield Structure(b"\x70", {})


class BoltScriptError(Exception):

    pass


class Line:

    script = None   # TODO - make context-free

    line_no = None

    async def action(self, actor):
        pass


class ClientLine(Line):

    pass


class ServerLine(Line):

    pass


class ClientMessageLine(ClientLine):

    def __init__(self, tag_name, *fields):
        self.tag_name = tag_name
        self.fields = fields

    def __str__(self):
        return "C: %s %s" % (self.tag_name, " ".join(map(repr, self.fields)))

    async def action(self, actor):
        await self.default_action(actor, self)

    @classmethod
    async def default_action(cls, actor, line=None):
        # TODO: improve the flow of logic here
        script = actor.script
        request = None
        c_msg = None
        more = True
        while more:
            try:
                request = await actor.stream.read_message()
            except IncompleteReadError as error:
                if not line and error.expected == 2 and error.partial == b"":
                    # Likely failed reading a new chunk header, and we're not
                    # waiting for anything specific anyway, so just exit quietly.
                    return
                else:
                    raise
            tag = script.tag_name("C", request.tag)
            c_msg = ClientMessageLine(tag, *request.fields)
            c_msg.script = script
            if script.auto_match(request.tag):
                # Auto-matched
                actor.log("(AUTO) %s", c_msg)
                for response in script.on_auto_match(request):
                    tag = script.tag_name("S", response.tag)
                    s_msg = ServerMessageLine(tag, *response.fields)
                    s_msg.script = script
                    actor.log("(AUTO) %s", s_msg)
                    actor.stream.write_message(response)
                await actor.stream.drain()
            else:
                more = False
        if line and line.match(request):
            actor.log("%s", c_msg)
        else:
            actor.log("%s", c_msg)
            if line:
                raise ScriptMismatch("Expected «{}»\n"
                                     "Received «{}»".format(line, c_msg), line, c_msg)
            else:
                raise ScriptMismatch("Expected no more lines\n"
                                     "Received «{}»".format(c_msg), None, c_msg)

    def match(self, message):
        tag = self.script.tag("C", self.tag_name)
        return tag == message.tag and tuple(self.fields) == tuple(message.fields)


class ServerMessageLine(ServerLine):

    def __init__(self, tag_name, *fields):
        self.tag_name = tag_name
        self.fields = fields

    def __str__(self):
        return "S: %s %s" % (self.tag_name, " ".join(map(repr, self.fields)))

    async def action(self, actor):
        actor.log("%s", self)
        tag = self.script.tag("S", self.tag_name)
        actor.stream.write_message(Structure(tag, *self.fields))
        await actor.stream.drain()


class ServerRawBytesLine(ServerLine):

    def __init__(self, data):
        self.data = data

    def __str__(self):
        return "S: <RAW> %r" % (self.data,)

    async def action(self, actor):
        actor.log("%s", self)
        actor.writer.write(self.data)
        await actor.writer.drain()


class ServerSleepLine(ServerLine):

    def __init__(self, delay):
        self.delay = delay

    def __str__(self):
        return "S: <SLEEP> %r" % (self.delay,)

    async def action(self, actor):
        actor.log("%s", self)
        await sleep(self.delay)


class ServerExitLine(ServerLine):

    def __init__(self):
        pass

    def __str__(self):
        return "S: <EXIT>"

    async def action(self, actor):
        actor.log("%s", self)
        raise ServerExit()


class ScriptMismatch(Exception):

    script = None
    line_no = None

    def __init__(self, message, expected, received):
        super().__init__(message)
        self.expected = expected
        self.received = received


class ServerExit(Exception):

    pass
