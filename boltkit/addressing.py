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


from socket import getaddrinfo, SOCK_STREAM

from click import ParamType


class AddressList(list):

    def __init__(self, iterable=None, default_host=None, default_port=None):
        if isinstance(iterable, str):
            super().__init__(iterable.split())
        else:
            super().__init__(iterable or ())
        self.default_host = default_host
        self.default_port = default_port

    def __str__(self):
        return " ".join(map(format_addr, self))

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, str(self))

    def resolve(self, family=0):
        resolved = []
        for item in iter(self):
            parsed = parse_addr(item)
            host = parsed[0] or self.default_host
            port = parsed[1] or self.default_port
            resolved.extend(addr for _, _, _, _, addr in getaddrinfo(host, port, family, SOCK_STREAM))
        self[:] = resolved


class AddressListParamType(ParamType):

    name = "addr"

    def __init__(self, default_host=None, default_port=None):
        self.default_host = default_host or "localhost"
        self.default_port = default_port or 7687

    def convert(self, value, param, ctx):
        return AddressList(value, self.default_host, self.default_port)

    def __repr__(self):
        return 'HOST:PORT [HOST:PORT...]'


def parse_addr(addr):
    if isinstance(addr, str):
        return addr.partition(":")[::2]
    elif isinstance(addr, tuple):
        return addr
    else:
        raise TypeError("Cannot parse address of type "
                        "{}".format(addr.__class__.__name__))


def format_addr(addr):
    if isinstance(addr, str):
        return addr
    elif isinstance(addr, tuple):
        if len(addr) == 2:
            return "{}:{}".format(*addr)
        elif len(addr) == 4:
            return "[{}]:{}".format(*addr)
    return "?"
