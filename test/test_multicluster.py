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

from unittest import TestCase
from boltkit.multicluster import parse_args, parse_install_command


class ConnectionTestCase(TestCase):


    def test_parse_args_in_any_order(self):
        parsed = parse_args(["install", "3.4.0", "-p", "pAssw0rd", "-v",
                             "-d" "{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}",
                             "--path", "$HOME/multi-cluster/"])
        assert parsed.path == "$HOME/multi-cluster/"

    def test_parse_args_with_default(self):
        parsed = parse_args(["install", "3.4.0", "-p", "pAssw0rd", "-v",
                             "-d" "{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}"])
        assert parsed.path == "."

    def test_parse_install_args(self):
        parsed = parse_args(["install", "3.4.0", "--path", "$HOME/multi-cluster/", "-p", "pAssw0rd", "-v",
                             "-d" "{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}"])
        installed = parse_install_command(parsed)
        expected = {u"london":
                        {"verbose": True,
                         "version": "3.4.0",
                         "read_replica_count": 2,
                         "initial_port": 20000,
                         "password": "pAssw0rd",
                         "core_count": 3},
                    u"malmo":
                        {"verbose": True,
                         "version": "3.4.0",
                         "read_replica_count": 0,
                         "initial_port": 9001,
                         "password": "pAssw0rd",
                         "core_count": 5},
                    }
        self.assertDictEqual(installed, expected)

    def test_parse_empty_database_args(self):
        parsed = parse_args(["install", "3.4.0", "--path","$HOME/multi-cluster/", "-p", "pAssw0rd",
                             "-d" "{\"london\":{}, \"malmo\": {}}"])
        installed = parse_install_command(parsed)
        expected = {u"london":
                        {"verbose": False,
                         "version": "3.4.0",
                         "read_replica_count": 0,
                         "initial_port": 20000,
                         "password": "pAssw0rd",
                         "core_count": 3},
                    u"malmo":
                        {"verbose": False,
                         "version": "3.4.0",
                         "read_replica_count": 0,
                         "initial_port": 20100,
                         "password": "pAssw0rd",
                         "core_count": 3},
                    }
        self.assertDictEqual(installed, expected)
