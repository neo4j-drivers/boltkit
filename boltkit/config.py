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

from __future__ import print_function

from os.path import join as path_join

try:
    from urllib.request import urlopen, Request, HTTPError
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen, Request, HTTPError
    from urlparse import urlparse

CONF_DIR = "conf"
CONF_FILE = "neo4j.conf"

HTTP_URI_SETTING = "dbms.connector.http.address" # setting name for 3.0
HTTP_LISTEN_URI_SETTING = "dbms.connector.http.listen_address" # setting name starting from 3.1

BOLT_URI_SETTING = "dbms.connector.bolt.address" # setting name for 3.0
BOLT_LISTEN_URI_SETTING = "dbms.connector.bolt.listen_address" # setting name starting from 3.1

WINDOWS_SERVICE_NAME_SETTING = "dbms.windows_service_name"

DEFAULT_PAGE_CACHE_MEMORY = "50m"
DEFAULT_XMS_MEMORY = "300m"
DEFAULT_XMX_MEMORY = "500m"


def update(path, properties):
    config_file_path = _config_file_path(path)

    with open(config_file_path, "r") as f_in:
        lines = f_in.readlines()
    with open(config_file_path, "w") as f_out:
        for line in lines:
            for key, value in properties.items():
                if line.startswith(key + "=") or \
                        (line.startswith("#") and line[1:].lstrip().startswith(key + "=")):
                    f_out.write("%s=%s\n" % (key, value))
                    break
            else:
                f_out.write(line)


def extract_http_and_bolt_uris(path):
    config_file_path = _config_file_path(path)

    with open(config_file_path, "r") as f_in:
        lines = f_in.readlines()

    http_uri = None
    bolt_uri = None

    for line in lines:
        if HTTP_URI_SETTING in line or HTTP_LISTEN_URI_SETTING in line:
            if http_uri is not None:
                raise RuntimeError("Duplicated http uri configs found in %s" % config_file_path)

            http_uri = _parse_uri("http", line)

        if BOLT_URI_SETTING in line or BOLT_LISTEN_URI_SETTING in line:
            if bolt_uri is not None:
                raise RuntimeError("Duplicated bolt uri configs found in %s" % config_file_path)

            bolt_uri = _parse_uri("bolt", line)

    return (http_uri or urlparse("http://localhost:7474"),
            bolt_uri or urlparse("bolt://localhost:7687"))


def for_core(expected_core_cluster_size, initial_discovery_members, discovery_listen_address,
             transaction_listen_address, raft_listen_address, bolt_listen_address, http_listen_address,
             https_listen_address):
    config = {
        "dbms.mode": "CORE",
        "causal_clustering.expected_core_cluster_size": expected_core_cluster_size,
        "causal_clustering.initial_discovery_members": initial_discovery_members,
        "causal_clustering.discovery_listen_address": discovery_listen_address,
        "causal_clustering.transaction_listen_address": transaction_listen_address,
        "causal_clustering.raft_listen_address": raft_listen_address,
        "dbms.connector.bolt.listen_address": bolt_listen_address,
        "dbms.connector.http.listen_address": http_listen_address,
        "dbms.connector.https.listen_address": https_listen_address
    }
    config.update(_memory_config())
    return config


def for_read_replica(initial_discovery_members, bolt_listen_address, http_listen_address, https_listen_address):
    config = {
        "dbms.mode": "READ_REPLICA",
        "causal_clustering.initial_discovery_members": initial_discovery_members,
        "dbms.connector.bolt.listen_address": bolt_listen_address,
        "dbms.connector.http.listen_address": http_listen_address,
        "dbms.connector.https.listen_address": https_listen_address
    }
    config.update(_memory_config())
    return config


def extract_windows_service_name(path):
    config_file_path = _config_file_path(path)

    with open(config_file_path, "r") as f_in:
        lines = f_in.readlines()

    service_name = None

    for line in lines:
        if WINDOWS_SERVICE_NAME_SETTING in line:
            if service_name is not None:
                raise RuntimeError("Duplicated windows service name configs found in %s" % config_file_path)

            service_name = line.partition("=")[-1].strip()

    return service_name


def _memory_config():
    return {
        "dbms.memory.pagecache.size": DEFAULT_PAGE_CACHE_MEMORY,
        "dbms.memory.heap.initial_size": DEFAULT_XMS_MEMORY,
        "dbms.memory.heap.max_size": DEFAULT_XMX_MEMORY
    }


def _parse_uri(scheme, config_entry):
    uri = config_entry.partition("=")[-1].strip()

    if uri.startswith(":"):
        uri = scheme + "://localhost" + uri

    if not uri.startswith(scheme + "://"):
        uri = scheme + "://" + uri

    parsed_uri = urlparse(uri)

    if not parsed_uri.scheme or not parsed_uri.hostname or not parsed_uri.port:
        raise RuntimeError("Cannot parse uri from config '%s'" % uri)

    return parsed_uri


def _config_file_path(root):
    return path_join(root, CONF_DIR, CONF_FILE)
