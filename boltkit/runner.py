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

from .driver import Driver, string
from .watcher import watch


def run():
    watch("boltkit.connection")
    driver = Driver("bolt://localhost:7687", user="neo4j", password="password")
    session = driver.session()
    result = session.run(argv[1])
    while result.forward():
        print("\t".join(map(string, result.current())))
    session.close()
    driver.close()


if __name__ == "__main__":
    run()
