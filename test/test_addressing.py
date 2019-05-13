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


from socket import AF_INET, AF_INET6

from pytest import raises

from boltkit.addressing import AddressList


def test_ipv4_construction():
    a = AddressList([("127.0.0.1", 80)])
    assert a == [("127.0.0.1", 80)]


def test_ipv6_construction():
    a = AddressList([("::1", 80, 0, 0)])
    assert a == [("::1", 80, 0, 0)]


def test_illegal_type_in_construction():
    with raises(TypeError):
        _ = AddressList([object()])


def test_full_resolution():
    a = AddressList([("localhost", "http")])
    a.resolve()
    assert a == [('::1', 80, 0, 0), ('127.0.0.1', 80)]


def test_ipv4_only_resolution():
    a = AddressList([("localhost", "http")])
    a.resolve(family=AF_INET)
    assert a == [('127.0.0.1', 80)]


def test_ipv6_only_resolution():
    a = AddressList([("localhost", "http")])
    a.resolve(family=AF_INET6)
    assert a == [('::1', 80, 0, 0)]


def test_resolution_with_empty_host():
    a = AddressList([("", "http")])
    a.resolve()
    assert a == [('::1', 80, 0, 0), ('127.0.0.1', 80)]


def test_resolution_with_empty_port():
    a = AddressList([("localhost", "")])
    a.resolve()
    assert a == [('::1', 0, 0, 0), ('127.0.0.1', 0)]


def test_resolution_with_empty_host_and_port():
    a = AddressList([("", "")])
    a.resolve()
    assert a == [('::1', 0, 0, 0), ('127.0.0.1', 0)]


def test_resolution_with_defaults():
    a = AddressList([("", "")], default_host="127.0.0.1", default_port=80)
    a.resolve()
    assert a == [('127.0.0.1', 80)]


def test_parsing_empty_string():
    a = AddressList.parse("")
    assert a == []


def test_parsing_ipv4_address():
    a = AddressList.parse("127.0.0.1:80")
    assert a == [('127.0.0.1', '80')]


def test_parsing_ipv6_address():
    a = AddressList.parse("[::1]:80")
    assert a == [('::1', '80', 0, 0)]


def test_parsing_multiple_addresses():
    a = AddressList.parse("127.0.0.1:80 [::1]:80")
    assert a == [('127.0.0.1', '80'), ('::1', '80', 0, 0)]


def test_parsing_host_and_port():
    a = AddressList.parse("localhost:http")
    assert a == [('localhost', 'http')]


def test_parsing_host_only():
    a = AddressList.parse("localhost")
    assert a == [('localhost', '')]


def test_parsing_port_only():
    a = AddressList.parse(":http")
    assert a == [('', 'http')]


def test_parsing_empty_host_and_port():
    a = AddressList.parse(":")
    assert a == [('', '')]


def test_illegal_type_in_parsing():
    with raises(TypeError):
        _ = AddressList.parse(object())


def test_string_repr():
    a = AddressList([('127.0.0.1', '80'), ('::1', '80', 0, 0)])
    assert str(a) == "127.0.0.1:80 [::1]:80"
