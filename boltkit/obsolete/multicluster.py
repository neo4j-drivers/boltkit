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

from os.path import join as path_join, realpath
from boltkit.obsolete.cluster import Cluster, create_sub_commands_description, DEFAULT_INITIAL_PORT
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from sys import stderr, argv
from os import listdir
import json




class MultiCluster:

    def __init__(self, path, verbose=False):
        self.path = path
        self.verbose = verbose

    # param databases: pairs of db_name=(version, core_count, read_replica_count, initial_port, password, verbose)
    def install(self, **database):
        initial_members = []

        clusters = {}
        for name in database:
            cluster = Cluster(path_join(self.path, name))
            clusters[name] = cluster
            args = database[name]
            self.write("Database: %s, Settings: %s" % (name, args))
            self.write("\r\n")

            cluster.install(**args)
            initial_members.append(cluster.initial_discovery_members)

        initial_members_str = ','.join(initial_members)
        for name in clusters:
            properties = {
                "causal_clustering.initial_discovery_members": initial_members_str,
                "causal_clustering.database": name
            }
            clusters[name].update_config(properties)
        return realpath(self.path)

    # param timeout: timeout for starting each cluster in multi-cluster
    def start(self, timeout):
        cluster = self.scan_clusters()
        self.write("Found databases in path: %s" % cluster.keys())
        self.write("\r\n")

        members = ""
        for name in cluster:
            self.write("Start database %s with timeout %ss" % (name, timeout))
            self.write("\r\n")
            single_cluster_members = cluster[name].start(timeout, self.verbose)
            members = "%s\r\n%s" % (members, single_cluster_members)
        return members

    def stop(self, kill):
        cluster = self.scan_clusters()
        self.write("Found databases in path: %s" % cluster.keys())
        self.write("\r\n")

        for name in cluster:
            self.write("Killing database %s with kill=%s" % (name, kill))
            self.write("\r\n")
            cluster[name].stop(kill)

    def write(self, message):
        if self.verbose:
            stderr.write(message)

    def scan_clusters(self):
        clusters = {}
        for database in listdir(self.path):
            clusters[database] = Cluster(path_join(self.path, database))
        return clusters


def _execute_cluster_command(parsed):
    command = parsed.command
    cluster_ctrl = MultiCluster(parsed.path, parsed.verbose)
    if command == "install":
        database = parse_install_command(parsed)
        path = cluster_ctrl.install(**database)
        print(path)
    elif command == "start":
        cluster_info = cluster_ctrl.start(parsed.timeout)
        print(cluster_info)
    elif command == "stop":
        cluster_ctrl.stop(parsed.kill)
    else:
        raise RuntimeError("Unknown command %s" % command)


def parse_install_command(parsed):
    database_json = json.loads(parsed.database)
    database = {}
    i = 0
    for name in database_json:
        cluster_settings = database_json[name]
        read_replica_count = cluster_settings["r"] if "r" in cluster_settings else 0
        core_count = cluster_settings["c"] if "c" in cluster_settings else 3
        initial_port = cluster_settings["i"] if "i" in cluster_settings else DEFAULT_INITIAL_PORT + i * 100
        i = i + 1
        args = {
            "version": parsed.version,
            "core_count": core_count,
            "read_replica_count": read_replica_count,
            "initial_port": initial_port,
            "password": parsed.password,
            "verbose": parsed.verbose
        }
        database[name] = args
    return database


def multicluster():
    parsed = parse_args(argv[1:])
    _execute_cluster_command(parsed)


def parse_args(args):
    see_download_command = ("See neoctrl-download for details of supported environment variables.\r\n"
                            "\r\n"
                            "Report bugs to drivers@neo4j.com")

    parser = ArgumentParser(description="Operate Neo4j multi-cluster.\r\n",
                            epilog=see_download_command,
                            formatter_class=RawDescriptionHelpFormatter)

    sub_commands_with_description = {
        "install": "Download, install and configure multi-cluster",
        "start": "Start the multi-cluster located at the given path",
        "stop": "Stop the multi-cluster located at the given path"
    }

    subparsers = parser.add_subparsers(title="available sub-commands", dest="command",
                                       help="Commands are available",
                                       description=create_sub_commands_description(sub_commands_with_description))

    # install
    parser_install = subparsers.add_parser("install", epilog=see_download_command,
                                           description=
                                           sub_commands_with_description["install"] +
                                           "\r\n\r\nExample:\r\n"
                                           "  neoctrl-multicluster install 3.4.0 [--path $HOME/multi-cluster/]"
                                           " -p pAssw0rd [-v]"
                                           " -d '{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}'",
                                           formatter_class=RawDescriptionHelpFormatter)

    parser_install.add_argument("version", help="Neo4j server version")
    parser_install.add_argument("--path", default=".", dest="path", help="download destination path (default: .)")
    parser_install.add_argument("-p", "--password", required=True,
                                help="initial password of the initial admin user ('neo4j') for all cluster members")
    parser_install.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_install.add_argument("-d","--database", dest="database", required=True,
                                help="a json string describing the multi-cluster structure in the form of "
                                     "{database_name: {c:core_size, r:read_replica_size, i:initial_port},...} "
                                     "defaults: core_size=3, read_replia_size=0, initial_port=%d "
                                     "e.g. '{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}'"
                                     % DEFAULT_INITIAL_PORT)

    # start
    parser_start = subparsers.add_parser("start", epilog=see_download_command,
                                         description=
                                         sub_commands_with_description["start"] +
                                         "\r\n\r\nexample:\r\n"
                                         "  neoctrl-multicluster start [-v] $HOME/cluster/",
                                         formatter_class=RawDescriptionHelpFormatter)

    parser_start.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_start.add_argument("-t", "--timeout", default=180, dest="timeout", type=int,
                              help="startup timeout for each cluster inside the multicluster in seconds (default: 180)")
    parser_start.add_argument("path", nargs="?", default=".", help="multi-cluster location path (default: .)")

    # stop
    parser_stop = subparsers.add_parser("stop", epilog=see_download_command,
                                        description=
                                        sub_commands_with_description["stop"] +
                                        "\r\n\r\nexample:\r\n"
                                        "  neoctrl-multicluster stop [-v] $HOME/cluster/",
                                        formatter_class=RawDescriptionHelpFormatter)

    parser_stop.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_stop.add_argument("-k", "--kill", action="store_true",
                             help="forcefully kill all instances in the multi-cluster. Default: false")
    parser_stop.add_argument("path", nargs="?", default=".", help="multi-cluster location path (default: .)")

    return parser.parse_args(args)