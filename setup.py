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

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


packages = find_packages(exclude=("test", "test.*"))
package_metadata = {
    "name": "boltkit",
    "version": "1.0.13",
    "description": "Toolkit for Neo4j 3.0+ driver authors",
    "author": "Neo Technology",
    "author_email": "drivers@neo4j.com",
    "entry_points": {
        "console_scripts": [
            "boltrun = boltkit.runner:run",
            "boltstub = boltkit.server:stub",
            "neoctrl-download = boltkit.controller:download",
            "neoctrl-install = boltkit.controller:install",
            "neoctrl-cluster = boltkit.controller:cluster",
            "neoctrl-start = boltkit.controller:start",
            "neoctrl-stop = boltkit.controller:stop",
            "neoctrl-set-initial-password = boltkit.controller:set_initial_password",
            "neoctrl-create-user = boltkit.controller:create_user",
            "neoctrl-configure = boltkit.controller:configure",
            "neotest = boltkit.controller:test",
        ],
    },
    "packages": packages,
    "license": "Apache License, Version 2.0",
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Database",
        "Topic :: Software Development",
    ],
}

setup(**package_metadata)
