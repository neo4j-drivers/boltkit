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

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from genericpath import isdir
from itertools import count
from os import listdir
from os.path import join as path_join, realpath
from boltkit import config as config
from boltkit.controller import create_controller, wait_for_server

try:
    from urllib.request import HTTPError
except ImportError:
    from urllib2 import HTTPError

CORES_DIR = "cores"
CORE_DIR_FORMAT = "core-%d"
READ_REPLICAS_DIR = "read-replicas"
READ_REPLICA_DIR_FORMAT = "read-replica-%d"

DEFAULT_INITIAL_PORT = 20000


class Cluster:
    def __init__(self, path):
        self.path = path

    def install(self, version, core_count, read_replica_count, initial_port, password, verbose=False):
        try:
            package = create_controller().download("enterprise", version, self.path, verbose=verbose)
            port_gen = count(initial_port)

            self.initial_discovery_members = self._install_cores(self.path, package, core_count, port_gen)
            self._install_read_replicas(self.path, package, self.initial_discovery_members, read_replica_count, port_gen)
            self._set_initial_password(password)

            return realpath(self.path)

        except HTTPError as error:
            if error.code == 401:
                raise RuntimeError("Missing or incorrect authorization")
            elif error.code == 403:
                raise RuntimeError("Could not download package from %s (403 Forbidden)" % error.url)
            else:
                raise

    def start(self, timeout, verbose=False):
        member_info_array = self._foreach_cluster_member(self._cluster_member_start)

        for member_info in member_info_array:
            wait_for_server(member_info.http_uri.hostname, member_info.http_uri.port, timeout)

        return "\r\n".join(map(str, member_info_array))

    def stop(self, kill):
        self._foreach_cluster_member(self._cluster_member_kill if kill else self._cluster_member_stop)

    def update_config(self, properties):
        self._foreach_cluster_member(lambda path: config.update(path, properties))

    def _set_initial_password(self, password):
        self._foreach_cluster_member(lambda path: self._cluster_member_set_initial_password(path, password))

    @classmethod
    def _install_cores(cls, path, package, core_count, port_generator):
        discovery_listen_addresses = []
        transaction_listen_addresses = []
        raft_listen_addresses = []
        bolt_listen_addresses = []
        http_listen_addresses = []
        https_listen_addresses = []

        for core_idx in range(0, core_count):
            discovery_listen_addresses.append(_localhost(next(port_generator)))
            transaction_listen_addresses.append(_localhost(next(port_generator)))
            raft_listen_addresses.append(_localhost(next(port_generator)))
            bolt_listen_addresses.append(_localhost(next(port_generator)))
            http_listen_addresses.append(_localhost(next(port_generator)))
            https_listen_addresses.append(_localhost(next(port_generator)))

        initial_discovery_members = ",".join(discovery_listen_addresses)

        controller = create_controller()
        for core_idx in range(0, core_count):
            core_dir = CORE_DIR_FORMAT % core_idx
            core_member_path = path_join(path, CORES_DIR, core_dir)
            core_member_home = controller.extract(package, core_member_path)

            core_config = config.for_core(core_count, initial_discovery_members,
                                          discovery_listen_addresses[core_idx],
                                          transaction_listen_addresses[core_idx],
                                          raft_listen_addresses[core_idx],
                                          bolt_listen_addresses[core_idx],
                                          http_listen_addresses[core_idx],
                                          https_listen_addresses[core_idx])

            os_dependent_config = controller.os_dependent_config(core_dir)
            core_config.update(os_dependent_config)

            config.update(core_member_home, core_config)

        return initial_discovery_members

    @classmethod
    def _install_read_replicas(cls, path, package, initial_discovery_members, read_replica_count, port_generator):
        controller = create_controller()
        for read_replica_idx in range(0, read_replica_count):
            read_replica_dir = READ_REPLICA_DIR_FORMAT % read_replica_idx
            read_replica_path = path_join(path, READ_REPLICAS_DIR, read_replica_dir)
            read_replica_home = controller.extract(package, read_replica_path)

            bolt_listen_address = _localhost(next(port_generator))
            http_listen_address = _localhost(next(port_generator))
            https_listen_address = _localhost(next(port_generator))
            transaction_listen_address = _localhost(next(port_generator))
            discovery_listen_address = _localhost(next(port_generator))

            read_replica_config = config.for_read_replica(initial_discovery_members,
                                                          bolt_listen_address,
                                                          http_listen_address,
                                                          https_listen_address,
                                                          transaction_listen_address,
                                                          discovery_listen_address)

            os_dependent_config = controller.os_dependent_config(read_replica_dir)
            read_replica_config.update(os_dependent_config)

            config.update(read_replica_home, read_replica_config)

    @classmethod
    def _cluster_member_start(cls, path):
        controller = create_controller(path)
        return controller.start()

    @classmethod
    def _cluster_member_stop(cls, path):
        controller = create_controller(path)
        controller.stop(False)

    @classmethod
    def _cluster_member_kill(cls, path):
        controller = create_controller(path)
        controller.stop(True)

    @classmethod
    def _cluster_member_set_initial_password(cls, path, password):
        controller = create_controller(path)
        return controller.set_initial_password(password)

    def _foreach_cluster_member(self, action):
        core_results = self._foreach_cluster_root_dir(CORES_DIR, action)
        read_replica_results = self._foreach_cluster_root_dir(READ_REPLICAS_DIR, action)
        return core_results + read_replica_results

    def _foreach_cluster_root_dir(self, cluster_home_dir, action):
        results = []
        cluster_home_dir = path_join(self.path, cluster_home_dir)
        if isdir(cluster_home_dir):
            cluster_member_dirs = listdir(cluster_home_dir)
            for cluster_member_dir in cluster_member_dirs:
                if cluster_member_dir.startswith("."):
                    continue
                neo4j_dirs = listdir(path_join(cluster_home_dir, cluster_member_dir))
                for neo4j_dir in neo4j_dirs:
                    if neo4j_dir.startswith("neo4j"):
                        neo4j_path = path_join(cluster_home_dir, cluster_member_dir, neo4j_dir)
                        result = action(neo4j_path)
                        results.append(result)
                        break

        return results


def cluster():
    see_download_command = ("See neoctrl-download for details of supported environment variables.\r\n"
                            "\r\n"
                            "Report bugs to drivers@neo4j.com")

    parser = ArgumentParser(description="Operate Neo4j causal cluster.\r\n",
                            epilog=see_download_command,
                            formatter_class=RawDescriptionHelpFormatter)

    sub_commands_with_description = {
        "install": "Download, extract and configure causal cluster",
        "start": "Start the causal cluster located at the given path",
        "stop": "Stop the causal cluster located at the given path"
    }

    subparsers = parser.add_subparsers(title="available sub-commands", dest="command",
                                       help="commands are available",
                                       description=create_sub_commands_description(sub_commands_with_description))

    parser_install = subparsers.add_parser("install", epilog=see_download_command,
                                           description=
                                           sub_commands_with_description["install"] +
                                           "\r\n\r\nexample:\r\n"
                                           "  neoctrl-cluster install [-v] [-c 3] -p pAssw0rd 3.1.0 $HOME/cluster/",
                                           formatter_class=RawDescriptionHelpFormatter)

    parser_install.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_install.add_argument("version", help="Neo4j server version")
    parser_install.add_argument("-c", "--cores", default=3, dest="core_count", type=int,
                                help="number of core members in the cluster (default 3)")
    parser_install.add_argument("-r", "--read-replicas", default=0, dest="read_replica_count", type=int,
                                help="number of read replicas in the cluster (default 0)")
    parser_install.add_argument("-i", "--initial-port", default=DEFAULT_INITIAL_PORT, type=int,
                                dest="initial_port", help="initial port number for all used ports on all cluster "
                                                          "members. Each next port will simply be an increment of "
                                                          "the previous one (default %d)" % DEFAULT_INITIAL_PORT)
    parser_install.add_argument("-p", "--password", required=True,
                                help="initial password of the initial admin user ('neo4j') for all cluster members")
    parser_install.add_argument("path", nargs="?", default=".", help="download destination path (default: .)")

    parser_start = subparsers.add_parser("start", epilog=see_download_command,
                                         description=
                                         sub_commands_with_description["start"] +
                                         "\r\n\r\nexample:\r\n"
                                         "  neoctrl-cluster start $HOME/cluster/",
                                         formatter_class=RawDescriptionHelpFormatter)

    parser_start.add_argument("-t", "--timeout", default=180, dest="timeout", type=int,
                              help="startup timeout in seconds (default: 180)")

    parser_start.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    parser_stop = subparsers.add_parser("stop", epilog=see_download_command,
                                        description=
                                        sub_commands_with_description["stop"] +
                                        "\r\n\r\nexample:\r\n"
                                        "  neoctrl-cluster stop $HOME/cluster/",
                                        formatter_class=RawDescriptionHelpFormatter)

    parser_stop.add_argument("-k", "--kill", action="store_true",
                             help="forcefully kill all instances in the cluster")
    parser_stop.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    parsed = parser.parse_args()
    _execute_cluster_command(parsed)


def create_sub_commands_description(sub_commands_with_description):
    result = []

    for key, value in sub_commands_with_description.items():
        result.append("%-21s %s" % (key, value))

    return "\r\n".join(result)


def _execute_cluster_command(parsed):
    command = parsed.command
    cluster_ctrl = Cluster(parsed.path)
    if command == "install":
        path = cluster_ctrl.install(parsed.version, parsed.core_count, parsed.read_replica_count, parsed.initial_port,
                                    parsed.password, parsed.verbose)
        print(path)
    elif command == "start":
        cluster_info = cluster_ctrl.start(parsed.timeout)
        print(cluster_info)
    elif command == "stop":
        cluster_ctrl.stop(parsed.kill)
    else:
        raise RuntimeError("Unknown command %s" % command)


def _localhost(port):
    return "127.0.0.1:%d" % port
