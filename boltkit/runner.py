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

from os import getenv
from sys import argv

from .connector import connect
from .watcher import watch


def run():
    watch("boltkit.connector")
    host = getenv("NEO4J_HOST", "localhost")
    port = getenv("NEO4J_PORT", "7687")
    try:
        port_no = int(port)
    except (ValueError, TypeError):
        raise ValueError("Invalid port number %r" % port)
    address = (host, port_no)
    with connect(address, user=getenv("NEO4J_USER", "neo4j"), password=getenv("NEO4J_PASSWORD")) as cx:
        records = []
        cx.run(argv[1], {})
        cx.pull(-1, records)
        cx.send_all()
        cx.fetch_all()
        for record in records:
            print("\t".join(map(str, record)))


if __name__ == "__main__":
    run()
