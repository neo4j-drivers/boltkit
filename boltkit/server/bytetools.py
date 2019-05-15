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


def h(data):
    """ A small helper function to translate byte data into a human-readable hexadecimal
    representation. Each byte in the input data is converted into a two-character hexadecimal
    string and is joined to its neighbours with a colon character.

    This function is not essential to driver-building but is a great help when debugging,
    logging and writing doctests.

        >>> from boltkit.bytetools import h
        >>> h(b"\x03A~")
        '03:41:7E'

    Args:
        data: Input byte data as `bytes` or a `bytearray`.

    Returns:
        A textual representation of the input data.
    """
    return ":".join("{:02X}".format(b) for b in bytearray(data))
