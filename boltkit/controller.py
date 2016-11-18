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

import platform
from argparse import ArgumentParser, RawDescriptionHelpFormatter, REMAINDER
from base64 import b64encode
from hashlib import sha256
from os import getenv, makedirs, listdir
from os.path import join as path_join, normpath, realpath
from random import randint
from socket import create_connection
from subprocess import call, check_output
from sys import stderr, stdin
from time import sleep

import boltkit.config as config

try:
    from urllib.request import urlopen, Request, HTTPError
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen, Request, HTTPError
    from urlparse import urlparse


DIST_HOST = "dist.neo4j.org"

CORES_DIR = "cores"
CORE_DIR_FORMAT = "core-%d"
READ_REPLICAS_DIR = "read-replicas"
READ_REPLICA_DIR_FORMAT = "read-replica-%d"

INITIAL_DISCOVERY_PORT = 5000
INITIAL_TRANSACTION_PORT = 6000
INITIAL_RAFT_PORT = 7000
INITIAL_BOLT_PORT = 7687
INITIAL_HTTP_PORT = 7474
INITIAL_HTTPS_PORT = 6474


class Downloader(object):

    def __init__(self, path, verbose=False):
        self.path = path
        self.verbose = verbose

    def write(self, message):
        if self.verbose:
            stderr.write(message)

    def download_build(self, address, build_id, package):
        """ Download from TeamCity build server.
        """
        url = "https://%s/repository/download/%s/.lastSuccessful/%s" % (address, build_id, package)
        user = getenv("TEAMCITY_USER")
        password = getenv("TEAMCITY_PASSWORD")
        auth_token = b"Basic " + b64encode(("%s:%s" % (user, password)).encode("iso-8859-1"))
        request = Request(url, headers={"Authorization": auth_token})
        package_path = path_join(self.path, package)

        self.write("Downloading build from %s... " % url)
        fin = urlopen(request)
        try:
            try:
                makedirs(self.path)
            except OSError:
                pass
            with open(package_path, "wb") as fout:
                more = True
                while more:
                    data = fin.read(8192)
                    if data:
                        fout.write(data)
                    else:
                        more = False
        finally:
            fin.close()

        return package_path

    def download_dist(self, address, package):
        """ Download from public distributions.
        """
        url = "http://%s/%s" % (address, package)
        package_path = path_join(self.path, package)

        self.write("Downloading dist from %s... " % url)
        fin = urlopen(url)
        try:
            try:
                makedirs(self.path)
            except OSError:
                pass
            with open(package_path, "wb") as fout:
                more = True
                while more:
                    data = fin.read(8192)
                    if data:
                        fout.write(data)
                    else:
                        more = False
        finally:
            fin.close()

        return package_path

    def download(self, edition, version, package_format):
        version_parts = version.replace("-", ".").replace("_", ".").split(".")
        if len(version_parts) == 3:
            major, minor, patch = version_parts
            milestone = ""
            tag = ""
        elif len(version_parts) == 4:
            major, minor, patch, milestone = version_parts
            tag = ""
        elif len(version_parts) == 5:
            major, minor, patch, milestone, tag = version_parts
        else:
            raise ValueError("Unrecognised version %r" % version)

        # Derive template and package version
        if package_format in ("unix.tar.gz", "windows.zip"):
            template = "neo4j-%s-%s-%s"
            package_version = "%s.%s.%s" % (major, minor, patch)
            if milestone:
                package_version = "%s-%s" % (package_version, milestone)
            if tag:
                package_version = "%s-%s" % (package_version, tag)
        else:
            raise ValueError("Unknown format %r" % package_format)
        package = template % (edition, package_version, package_format)

        dist_address = getenv("DIST_HOST", DIST_HOST)
        build_address = getenv("TEAMCITY_HOST")
        if build_address:
            try:
                build_id = "Neo4j%s%s_Packaging" % (major, minor)
                package = self.download_build(build_address, build_id, package)
            except HTTPError:
                package = self.download_dist(dist_address, package)
        else:
            package = self.download_dist(dist_address, package)
        self.write("\r\n")

        return package


def wait_for_server(host, port, timeout=30):
    running = False
    t = 0
    while not running and t < timeout:
        try:
            s = create_connection((host, port or 7474))
        except IOError:
            sleep(1)
            t += 1
        else:
            s.close()
            running = True


def hex_bytes(data):
    return b"".join(b"%02X" % b for b in bytearray(data))


def user_record(user, password):
    salt = bytearray(randint(0x00, 0xFF) for _ in range(16))
    m = sha256()
    m.update(salt)
    m.update(password)
    return b"%s:SHA-256,%s,%s:" % (user, hex_bytes(m.digest()), hex_bytes(salt))


class Controller(object):

    package_format = None

    @classmethod
    def extract(cls, archive, path):
        raise NotImplementedError("Not yet supported for this platform")

    @classmethod
    def download(cls, edition, version, path, **kwargs):
        downloader = Downloader(normpath(path), verbose=kwargs.get("verbose"))
        path = downloader.download(edition, version, cls.package_format)
        return realpath(path)

    @classmethod
    def install(cls, edition, version, path, **kwargs):
        package = cls.download(edition, version, path, **kwargs)
        home = cls.extract(package, path)
        return realpath(home)

    def __init__(self, home, verbosity=0):
        self.home = home
        self.verbosity = verbosity

    def start(self, wait):
        raise NotImplementedError("Not yet supported for this platform")

    def stop(self):
        raise NotImplementedError("Not yet supported for this platform")

    def create_user(self, user, password):
        raise NotImplementedError("Not yet supported for this platform")

    def set_user_role(self, user, role):
        raise NotImplementedError("Not yet supported for this platform")

    def run(self, *args):
        raise NotImplementedError("Not yet supported for this platform")


class UnixController(Controller):

    package_format = "unix.tar.gz"

    @classmethod
    def extract(cls, archive, path):
        from tarfile import TarFile
        with TarFile.open(archive) as files:
            files.extractall(path)
            return path_join(path, files.getnames()[0])

    def start(self, wait=True):
        http_uri, bolt_uri = config.extract_http_and_bolt_uris(self.home)
        check_output([path_join(self.home, "bin", "neo4j"), "start"])
        if wait:
            wait_for_server(http_uri.hostname, http_uri.port)
        return InstanceInfo(http_uri, bolt_uri, self.home)

    def stop(self):
        check_output([path_join(self.home, "bin", "neo4j"), "stop"])

    def create_user(self, user, password):
        data_dbms = path_join(self.home, "data", "dbms")
        try:
            makedirs(data_dbms)
        except OSError:
            pass
        with open(path_join(data_dbms, "auth"), "a") as f:
            f.write(user_record(user, password))
            f.write(b"\r\n")

    def set_user_role(self, user, role):
        data_dbms = path_join(self.home, "data", "dbms")
        try:
            makedirs(data_dbms)
        except OSError:
            pass
        lines = []
        try:
            with open(path_join(data_dbms, "roles"), "r") as f:
                for line in f:
                    lines.append(line.rstrip())
        except IOError:
            lines += ["admin:", "architect:", "publisher:", "reader:"]
        for i, line in enumerate(lines):
            if line.startswith(role + ":"):
                users = line[6:].split(",")
                users.append(user)
                line = role + ":" + ",".join(users)
            lines[i] = line
        with open(path_join(data_dbms, "roles"), "w") as f:
            for line in lines:
                f.write(line)
                f.write("\n")

    def run(self, *args):
        return call(args)


class WindowsController(Controller):

    package_format = "windows.zip"

    @classmethod
    def extract(cls, archive, path):
        from zipfile import ZipFile
        with ZipFile(archive, 'r') as files:
            files.extractall(path)
            return path_join(path, files.namelist()[0])

    def start(self, wait=True):
        raise NotImplementedError("Windows support not complete")

    def stop(self):
        raise NotImplementedError("Windows support not complete")

    def create_user(self, user, password):
        raise NotImplementedError("Windows support not complete")

    def set_user_role(self, user, role):
        raise NotImplementedError("Windows support not complete")

    def run(self, *args):
        raise NotImplementedError("Windows support not complete")


class Cluster:
    def __init__(self, path):
        self.path = path

    def install(self, version, core_count, read_replica_count, verbose=False):
        try:
            package = _create_controller().download("enterprise", version, self.path, verbose=verbose)

            initial_discovery_members = self._install_cores(self.path, package, core_count)
            self._install_read_replicas(self.path, package, initial_discovery_members, core_count, read_replica_count)

            return realpath(self.path)

        except HTTPError as error:
            if error.code == 401:
                raise RuntimeError("Missing or incorrect authorization")
            elif error.code == 403:
                raise RuntimeError("Could not download package from %s (403 Forbidden)" % error.url)
            else:
                raise

    def start(self):
        member_info_array = self._foreach_cluster_member(self._cluster_member_start)

        for member_info in member_info_array:
            wait_for_server(member_info.http_uri.hostname, member_info.http_uri.port)

        return "\r\n".join(map(str, member_info_array))

    def stop(self):
        self._foreach_cluster_member(self._cluster_member_stop)

    @classmethod
    def _install_cores(cls, path, package, core_count):
        discovery_listen_addresses = []
        transaction_listen_addresses = []
        raft_listen_addresses = []
        bolt_listen_addresses = []
        http_listen_addresses = []
        https_listen_addresses = []

        for core_idx in range(0, core_count):
            discovery_listen_addresses.append(localhost(INITIAL_DISCOVERY_PORT + core_idx))
            transaction_listen_addresses.append(localhost(INITIAL_TRANSACTION_PORT + core_idx))
            raft_listen_addresses.append(localhost(INITIAL_RAFT_PORT + core_idx))
            bolt_listen_addresses.append(localhost(INITIAL_BOLT_PORT + core_idx))
            http_listen_addresses.append(localhost(INITIAL_HTTP_PORT + core_idx))
            https_listen_addresses.append(localhost(INITIAL_HTTPS_PORT + core_idx))

        initial_discovery_members = ",".join(discovery_listen_addresses)

        for core_idx in range(0, core_count):
            core_member_path = path_join(path, CORES_DIR, CORE_DIR_FORMAT % core_idx)
            core_member_home = _create_controller().extract(package, core_member_path)

            core_config = config.for_core(core_count, initial_discovery_members,
                                          discovery_listen_addresses[core_idx],
                                          transaction_listen_addresses[core_idx],
                                          raft_listen_addresses[core_idx],
                                          bolt_listen_addresses[core_idx],
                                          http_listen_addresses[core_idx],
                                          https_listen_addresses[core_idx])

            config.update(core_member_home, core_config)

        return initial_discovery_members

    @classmethod
    def _install_read_replicas(cls, path, package, initial_discovery_members, core_count, read_replica_count):
        first_bolt_port = INITIAL_BOLT_PORT + core_count
        first_http_port = INITIAL_HTTP_PORT + core_count
        first_https_port = INITIAL_HTTPS_PORT + core_count

        for read_replica_idx in range(0, read_replica_count):
            read_replica_path = path_join(path, READ_REPLICAS_DIR, READ_REPLICA_DIR_FORMAT % read_replica_idx)
            read_replica_home = _create_controller().extract(package, read_replica_path)

            bolt_listen_address = localhost(first_bolt_port + read_replica_idx)
            http_listen_address = localhost(first_http_port + read_replica_idx)
            https_listen_address = localhost(first_https_port + read_replica_idx)

            read_replica_config = config.for_read_replica(initial_discovery_members, bolt_listen_address,
                                                          http_listen_address, https_listen_address)

            config.update(read_replica_home, read_replica_config)


    @classmethod
    def _cluster_member_start(cls, path):
        controller = _create_controller(path)
        return controller.start(False)

    @classmethod
    def _cluster_member_stop(cls, path):
        controller = _create_controller(path)
        controller.stop()

    def _foreach_cluster_member(self, action):
        core_results = self._foreach_cluster_root_dir(CORES_DIR, action)
        read_replica_results = self._foreach_cluster_root_dir(READ_REPLICAS_DIR, action)
        return core_results + read_replica_results

    def _foreach_cluster_root_dir(self, folder, action):
        results = []

        cluster_member_dirs = listdir(path_join(self.path, folder))
        for cluster_member_dir in cluster_member_dirs:
            neo4j_dirs = listdir(path_join(self.path, folder, cluster_member_dir))
            for neo4j_dir in neo4j_dirs:
                if neo4j_dir.startswith("neo4j"):
                    neo4j_path = path_join(self.path, folder, cluster_member_dir, neo4j_dir)
                    result = action(neo4j_path)
                    results.append(result)
                    break

        return results


class InstanceInfo:
    def __init__(self, http_uri, bolt_uri, path):
        self.http_uri = http_uri
        self.bolt_uri = bolt_uri
        self.path = path

    def http_uri_str(self):
        return self.http_uri.geturl().strip()

    def bolt_uri_str(self):
        return self.bolt_uri.geturl().strip()

    def __str__(self):
        return "%s %s %s" % (self.http_uri_str(), self.bolt_uri_str(), self.path)


def download():
    parser = ArgumentParser(
        description="Download a Neo4j server package for the current platform.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-download -e 3.1.0-M09 $HOME/servers/",
        epilog="environment variables:\r\n"
               "  DIST_HOST         name of distribution server (default: %s)\r\n"
               "  TEAMCITY_HOST     name of build server (optional)\r\n"
               "  TEAMCITY_USER     build server user name (optional)\r\n"
               "  TEAMCITY_PASSWORD build server password (optional)\r\n"
               "\r\n"
               "If TEAMCITY_* environment variables are set, the build server will be checked\r\n"
               "for the package before the distribution server. Note that supplying a local\r\n"
               "alternative DIST_HOST can help reduce test timings on a slow network.\r\n"
               "\r\n"
               "Report bugs to drivers@neo4j.com" % (DIST_HOST,),
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-e", "--enterprise", action="store_true",
                        help="select Neo4j Enterprise Edition (default: Community)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("version", help="Neo4j server version")
    parser.add_argument("path", nargs="?", default=".",
                        help="download destination path (default: .)")
    parsed = parser.parse_args()
    edition = "enterprise" if parsed.enterprise else "community"
    controller = _create_controller()
    try:
        home = controller.download(edition, parsed.version, parsed.path, verbose=parsed.verbose)
    except HTTPError as error:
        if error.code == 401:
            stderr.write("ERROR: Missing or incorrect authorization\r\n")
            exit(1)
        elif error.code == 403:
            stderr.write("ERROR: Could not download package from %s "
                         "(403 Forbidden)\r\n" % error.url)
            exit(1)
        else:
            raise
    else:
        print(home)


def _install(edition, version, path, **kwargs):
    controller = _create_controller()
    try:
        home = controller.install(edition, version.strip(), path, **kwargs)
    except HTTPError as error:
        if error.code == 401:
            raise RuntimeError("Missing or incorrect authorization")
        elif error.code == 403:
            raise RuntimeError("Could not download package from %s (403 Forbidden)" % error.url)
        else:
            raise
    else:
        return home


def install():
    parser = ArgumentParser(
        description="Download and extract a Neo4j server package for the current platform.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-install -e 3.1.0-M09 $HOME/servers/",
        epilog="See neoctrl-download for details of supported environment variables.\r\n"
               "\r\n"
               "Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-e", "--enterprise", action="store_true",
                        help="select Neo4j Enterprise Edition (default: Community)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("version", help="Neo4j server version")
    parser.add_argument("path", nargs="?", default=".",
                        help="download destination path (default: .)")
    parsed = parser.parse_args()
    home = _install("enterprise" if parsed.enterprise else "community",
                    parsed.version, parsed.path, verbose=parsed.verbose)
    print(home)


def cluster():
    see_download_command = ("See neoctrl-download for details of supported environment variables.\r\n"
                            "\r\n"
                            "Report bugs to drivers@neo4j.com")

    parser = ArgumentParser(
        description="Operate Neo4j causal cluster.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-cluster start 3.1.0-M09 $HOME/servers/",
        epilog=see_download_command,
        formatter_class=RawDescriptionHelpFormatter)

    subparsers = parser.add_subparsers(help="available sub-commands", dest="command")

    parser_install = subparsers.add_parser("install", epilog=see_download_command,
                                           description="Download, extract and configure causal cluster.\r\n"
                                                       "\r\n"
                                                       "example:\r\n"
                                                       "  neoctrl-cluster install [-v] 3.1.0 3 $HOME/cluster/",
                                           formatter_class=RawDescriptionHelpFormatter)

    parser_install.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_install.add_argument("version", help="Neo4j server version")
    parser_install.add_argument("-c", "--cores", default="3", dest="core_count",
                                help="Number of core members in the cluster (default 3)")
    parser_install.add_argument("-r", "--read-replicas", default="0", dest="read_replica_count",
                                help="Number of read replicas in the cluster (default 0)")
    parser_install.add_argument("path", nargs="?", default=".", help="download destination path (default: .)")

    parser_start = subparsers.add_parser("start", epilog=see_download_command,
                                         description="Start the causal cluster located at the given path.\r\n"
                                                     "\r\n"
                                                     "example:\r\n"
                                                     "  neoctrl-cluster start $HOME/cluster/",
                                         formatter_class=RawDescriptionHelpFormatter)

    parser_start.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    parser_stop = subparsers.add_parser("stop", epilog=see_download_command,
                                        description="Stop the causal cluster located at the given path.\r\n"
                                                    "\r\n"
                                                    "example:\r\n"
                                                    "  neoctrl-cluster stop $HOME/cluster/",
                                        formatter_class=RawDescriptionHelpFormatter)

    parser_stop.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    parsed = parser.parse_args()

    cluster_ctrl = Cluster(parsed.path)
    command = parsed.command
    if command == "install":
        core_count = int(parsed.core_count)
        read_replica_count = int(parsed.read_replica_count)
        path = cluster_ctrl.install(parsed.version, core_count, read_replica_count, parsed.verbose)
        print(path)
    elif command == "start":
        cluster_info = cluster_ctrl.start()
        print(cluster_info)
    elif command == "stop":
        cluster_ctrl.stop()
    else:
        raise RuntimeError("Unknown command %s" % command)


def start():
    parser = ArgumentParser(
        description="Start an installed Neo4j server instance.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-start $HOME/servers/neo4j-community-3.0.0",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("home", nargs="?", default=".",help="Neo4j server directory (default: .)")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        controller = WindowsController(parsed.home, 1 if parsed.verbose else 0)
    else:
        controller = UnixController(parsed.home, 1 if parsed.verbose else 0)
    instance_info = controller.start()

    print(instance_info.http_uri_str())
    print(instance_info.bolt_uri_str())


def stop():
    parser = ArgumentParser(
        description="Stop an installed Neo4j server instance.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-stop $HOME/servers/neo4j-community-3.0.0",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        controller = WindowsController(parsed.home, 1 if parsed.verbose else 0)
    else:
        controller = UnixController(parsed.home, 1 if parsed.verbose else 0)
    controller.stop()


def create_user():
    parser = ArgumentParser(
        description="Create a new Neo4j user.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-create-user $HOME/servers/neo4j-community-3.0.0 bob s3cr3t",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parser.add_argument("user", help="name of new user")
    parser.add_argument("password", help="password for new user")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        controller = WindowsController(parsed.home, 1 if parsed.verbose else 0)
    else:
        controller = UnixController(parsed.home, 1 if parsed.verbose else 0)
    user = parsed.user.encode(stdin.encoding or "utf-8")
    password = parsed.password.encode(stdin.encoding or "utf-8")
    controller.create_user(user, password)


def configure():
    parser = ArgumentParser(
        description="Update Neo4j server configuration.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-configure . dbms.security.auth_enabled=false",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parser.add_argument("items", metavar="key=value", nargs="+", help="key/value assignment")
    parsed = parser.parse_args()
    properties = dict(item.partition("=")[0::2] for item in parsed.items)
    config.update(parsed.home, properties)


def test():
    parser = ArgumentParser(
        description="Run tests against a Neo4j server.\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neotest -e 3.1.0-M09 test/run/ runtests.sh",
        epilog="See neoctrl-download for details of supported environment variables.\r\n"
               "\r\n"
               "Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-e", "--enterprise", action="store_true",
                        help="select Neo4j Enterprise Edition (default: Community)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="show more detailed output")
    parser.add_argument("versions", help="Neo4j server versions (colon-separated)")
    parser.add_argument("path", help="installation path")
    parser.add_argument("command", help="command to execute test")
    parser.add_argument("args", nargs=REMAINDER, help="arguments for test execution")
    parsed = parser.parse_args()
    exit_status = 0
    for version in parsed.versions.split(":"):
        print("\x1b[33;1m************************************************************\x1b[0m")
        print("\x1b[33;1m*** RUNNING TESTS AGAINST NEO4J SERVER %s\x1b[0m" % version)
        print("\x1b[33;1m************************************************************\x1b[0m")
        print()
        home = _install("enterprise" if parsed.enterprise else "community",
                        version, parsed.path, verbose=parsed.verbose)
        if platform.system() == "Windows":
            controller = WindowsController(home, 1 if parsed.verbose else 0)
        else:
            controller = UnixController(home, 1 if parsed.verbose else 0)
        controller.create_user("neotest", "neotest")
        controller.set_user_role("neotest", "admin")
        try:
            controller.start()
            exit_status = controller.run(parsed.command, *parsed.args)
        finally:
            controller.stop()
        print()
        if exit_status != 0:
            break
    exit(exit_status)


def _create_controller(path=None):
    return WindowsController(path) if platform.system() == "Windows" else UnixController(path)


def localhost(port):
    return "localhost:%d" % port

# todo:
#  - add read replicas dynamically
#  - hard kill of instances
