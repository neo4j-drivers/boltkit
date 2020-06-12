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

from sys import argv

from .driver import Connection
from .watcher import watch


def run():
    watch("boltkit.driver")
    with Connection.open(("localhost", 9001), auth=("neo4j", "password"), bolt_versions=[4.1]) as cx:
        records = []
        cx.run(argv[1], {})
        cx.pull(-1, -1, records)
        cx.send_all()
        cx.fetch_all()
        for record in records:
            print("\t".join(map(str, record)))


if __name__ == "__main__":
    run()
