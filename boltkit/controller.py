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
from itertools import count
from os import getenv, makedirs, listdir
from os.path import join as path_join, normpath, realpath, isdir, isfile, getsize
from random import randint
from socket import create_connection
from subprocess import call, check_output, CalledProcessError
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

DEFAULT_INITIAL_PORT = 20000


def bstr(s):
    return s if isinstance(s, (bytes, bytearray)) else s.encode("UTF-8")


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


def wait_for_server(host, port, timeout=90):
    port = port or 7474
    running = False
    t = 0
    while not running and t < timeout:
        try:
            s = create_connection((host, port))
        except IOError:
            sleep(1)
            t += 1
        else:
            s.close()
            running = True

    if not running:
        raise RuntimeError("Server %s:%d did not become available in %d seconds" % (host, port, timeout))


def hex_bytes(data):
    return "".join("%02X" % b for b in bytearray(data))


def user_record(user, password):
    salt = bytearray(randint(0x00, 0xFF) for _ in range(16))
    m = sha256()
    m.update(salt)
    m.update(bstr(password))
    return bstr("%s:SHA-256,%s,%s:" % (user, hex_bytes(m.digest()), hex_bytes(salt)))


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

    @classmethod
    def os_dependent_config(cls, instance_id):
        raise NotImplementedError("Not yet supported for this platform")

    def __init__(self, home, verbosity=0):
        self.home = home
        self.verbosity = verbosity

    def start(self, wait=True):
        raise NotImplementedError("Not yet supported for this platform")

    def stop(self, kill=False):
        raise NotImplementedError("Not yet supported for this platform")

    def create_user(self, user, password):
        data_dbms = path_join(self.home, "data", "dbms")
        try:
            makedirs(data_dbms)
        except OSError:
            pass
        with open(path_join(data_dbms, "auth"), "ab") as f:
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

        role_set = False
        for i, line in enumerate(lines):
            split = line.split(":")
            role_str = split[0]
            users_str = split[1]

            if role == role_str:
                users = users_str.split(",") if users_str else []
                users.append(user)
                lines[i] = role + ":" + ",".join(users)
                role_set = True

        if not role_set:
            lines.append(role + ":" + user)

        with open(path_join(data_dbms, "roles"), "w") as f:
            for line in lines:
                f.write(line)
                f.write("\n")

    def set_initial_password(self, password):
        live_users_detected = False

        if self._neo4j_admin_available():
            neo4j_admin_path = path_join(self.home, "bin", self._neo4j_admin_script_name())
            output = _invoke([neo4j_admin_path, "set-initial-password", password])
            live_users_detected = "password was not set" in output.lower()
        else:
            if self._auth_file_exists():
                live_users_detected = True
            else:
                self.create_user("neo4j", password)

        if live_users_detected:
            raise RuntimeError("Can't set initial password, live Neo4j-users were detected")

    def _neo4j_admin_available(self):
        try:
            neo4j_admin_path = path_join(self.home, "bin", self._neo4j_admin_script_name())
            output = _invoke([neo4j_admin_path, "help"])
            return "set-initial-password" in output
        except CalledProcessError:
            return False

    def _auth_file_exists(self):
        auth_file = path_join(self.home, "data", "dbms", "auth")
        return isfile(auth_file) and getsize(auth_file) > 0

    @classmethod
    def _neo4j_admin_script_name(cls):
        raise NotImplementedError("Not yet supported for this platform")


class UnixController(Controller):

    package_format = "unix.tar.gz"

    @classmethod
    def extract(cls, archive, path):
        from tarfile import TarFile
        with TarFile.open(archive) as files:
            files.extractall(path)
            return path_join(path, files.getnames()[0])

    @classmethod
    def os_dependent_config(cls, instance_id):
        return {}

    def start(self, wait=True):
        http_uri, bolt_uri = config.extract_http_and_bolt_uris(self.home)
        _invoke([path_join(self.home, "bin", "neo4j"), "start"])
        if wait:
            wait_for_server(http_uri.hostname, http_uri.port)
        return InstanceInfo(http_uri, bolt_uri, self.home)

    def stop(self, kill=False):
        if kill:
            output = _invoke([path_join(self.home, "bin", "neo4j"), "status"])
            if output.startswith("Neo4j is running"):
                pid = output.split(" ")[-1].strip()
                _invoke(["kill", "-9", pid])
            else:
                raise RuntimeError("Neo4j is not running")
        else:
            _invoke([path_join(self.home, "bin", "neo4j"), "stop"])

    @classmethod
    def _neo4j_admin_script_name(cls):
        return "neo4j-admin"


class WindowsController(Controller):

    package_format = "windows.zip"

    @classmethod
    def extract(cls, archive, path):
        from zipfile import ZipFile
        with ZipFile(archive, 'r') as files:
            files.extractall(path)
            return path_join(path, files.namelist()[0])

    @classmethod
    def os_dependent_config(cls, instance_id):
        return {config.WINDOWS_SERVICE_NAME_SETTING: instance_id}

    def start(self, wait=True):
        http_uri, bolt_uri = config.extract_http_and_bolt_uris(self.home)
        _invoke([path_join(self.home, "bin", "neo4j.bat"), "install-service"])
        _invoke([path_join(self.home, "bin", "neo4j.bat"), "start"])
        if wait:
            wait_for_server(http_uri.hostname, http_uri.port)
        return InstanceInfo(http_uri, bolt_uri, self.home)

    def stop(self, kill=False):
        if kill:
            windows_service_name = config.extract_windows_service_name(self.home)
            sc_output = _invoke(["sc", "queryex", windows_service_name])
            pid = None
            for line in sc_output.splitlines():
                line = line.strip()
                if line.startswith("PID"):
                    pid = line.split(":")[-1]

            if pid is None:
                raise RuntimeError("Unable to get PID")

            _invoke(["taskkill", "/f", "/pid", pid])
        else:
            _invoke([path_join(self.home, "bin", "neo4j.bat"), "stop"])

        _invoke([path_join(self.home, "bin", "neo4j.bat"), "uninstall-service"])

    @classmethod
    def _neo4j_admin_script_name(cls):
        return "neo4j-admin.bat"


class Cluster:
    def __init__(self, path):
        self.path = path

    def install(self, version, core_count, read_replica_count, initial_port, password, verbose=False):
        try:
            package = _create_controller().download("enterprise", version, self.path, verbose=verbose)
            port_gen = count(initial_port)

            initial_discovery_members = self._install_cores(self.path, package, core_count, port_gen)
            self._install_read_replicas(self.path, package, initial_discovery_members, read_replica_count, port_gen)
            self._set_initial_password(password)

            return realpath(self.path)

        except HTTPError as error:
            if error.code == 401:
                raise RuntimeError("Missing or incorrect authorization")
            elif error.code == 403:
                raise RuntimeError("Could not download package from %s (403 Forbidden)" % error.url)
            else:
                raise

    def start(self, timeout):
        member_info_array = self._foreach_cluster_member(self._cluster_member_start)

        for member_info in member_info_array:
            wait_for_server(member_info.http_uri.hostname, member_info.http_uri.port, timeout)

        return "\r\n".join(map(str, member_info_array))

    def stop(self, kill):
        self._foreach_cluster_member(self._cluster_member_kill if kill else self._cluster_member_stop)

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

        controller = _create_controller()
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
        controller = _create_controller()
        for read_replica_idx in range(0, read_replica_count):
            read_replica_dir = READ_REPLICA_DIR_FORMAT % read_replica_idx
            read_replica_path = path_join(path, READ_REPLICAS_DIR, read_replica_dir)
            read_replica_home = controller.extract(package, read_replica_path)

            bolt_listen_address = _localhost(next(port_generator))
            http_listen_address = _localhost(next(port_generator))
            https_listen_address = _localhost(next(port_generator))

            read_replica_config = config.for_read_replica(initial_discovery_members, bolt_listen_address,
                                                          http_listen_address, https_listen_address)

            os_dependent_config = controller.os_dependent_config(read_replica_dir)
            read_replica_config.update(os_dependent_config)

            config.update(read_replica_home, read_replica_config)

    @classmethod
    def _cluster_member_start(cls, path):
        controller = _create_controller(path)
        return controller.start(False)

    @classmethod
    def _cluster_member_stop(cls, path):
        controller = _create_controller(path)
        controller.stop(False)

    @classmethod
    def _cluster_member_kill(cls, path):
        controller = _create_controller(path)
        controller.stop(True)

    @classmethod
    def _cluster_member_set_initial_password(cls, path, password):
        controller = _create_controller(path)
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
                neo4j_dirs = listdir(path_join(self.path, cluster_home_dir, cluster_member_dir))
                for neo4j_dir in neo4j_dirs:
                    if neo4j_dir.startswith("neo4j"):
                        neo4j_path = path_join(self.path, cluster_home_dir, cluster_member_dir, neo4j_dir)
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
                                       description=_create_sub_commands_description(sub_commands_with_description))

    parser_install = subparsers.add_parser("install", epilog=see_download_command,
                                           description=
                                           sub_commands_with_description["install"] +
                                           "\r\n\r\nexample:\r\n"
                                           "  neoctrl-cluster install [-v] 3.1.0 3 $HOME/cluster/",
                                           formatter_class=RawDescriptionHelpFormatter)

    parser_install.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_install.add_argument("version", help="Neo4j server version")
    parser_install.add_argument("-c", "--cores", default="3", dest="core_count",
                                help="Number of core members in the cluster (default 3)")
    parser_install.add_argument("-r", "--read-replicas", default="0", dest="read_replica_count",
                                help="Number of read replicas in the cluster (default 0)")
    parser_install.add_argument("-i", "--initial-port", default=str(DEFAULT_INITIAL_PORT),
                                dest="initial_port", help="Initial port number for all used ports on all cluster "
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

    parser_start.add_argument("-t", "--timeout", default="180", dest="timeout",
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


def _create_sub_commands_description(sub_commands_with_description):
    result = []

    for key, value in sub_commands_with_description.items():
        result.append("%-21s %s" % (key, value))

    return "\r\n".join(result)


def _execute_cluster_command(parsed):
    command = parsed.command
    cluster_ctrl = Cluster(parsed.path)
    if command == "install":
        core_count = _parse_int(parsed.core_count, "core count")
        read_replica_count = _parse_int(parsed.read_replica_count, "read replica count")
        initial_port = _parse_int(parsed.initial_port, "initial port")
        path = cluster_ctrl.install(parsed.version, core_count, read_replica_count, initial_port, parsed.password,
                                    parsed.verbose)
        print(path)
    elif command == "start":
        startup_timeout = _parse_int(parsed.timeout, "timeout")
        cluster_info = cluster_ctrl.start(startup_timeout)
        print(cluster_info)
    elif command == "stop":
        cluster_ctrl.stop(parsed.kill)
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
    parser.add_argument("-k", "--kill", action="store_true",
                        help="forcefully kill the instance")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        controller = WindowsController(parsed.home, 1 if parsed.verbose else 0)
    else:
        controller = UnixController(parsed.home, 1 if parsed.verbose else 0)
    controller.stop(parsed.kill)


def set_initial_password():
    parser = ArgumentParser(
        description="Sets the initial password of the initial admin user ('neo4j').\r\n"
                    "\r\n"
                    "example:\r\n"
                    "  neoctrl-set-initial-password newPassword $HOME/servers/neo4j-community-3.0.0",
        epilog="Report bugs to drivers@neo4j.com",
        formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("password", help="password for the admin user")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parsed = parser.parse_args()
    controller = _create_controller(parsed.home)
    controller.set_initial_password(parsed.password)


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
            exit_status = call([parsed.command] + parsed.args)
        except OSError:
            raise RuntimeError("Unable to run command %r with "
                               "arguments %r" % (parsed.command, parsed.args))
        finally:
            controller.stop()
        print("")
        if exit_status != 0:
            break
    exit(exit_status)


def _invoke(command):
    try:
        return check_output(command)
    except CalledProcessError as error:
        print("Command failed.\r\nError code: %s\r\nOutput:\r\n%s\n\r" % (str(error.returncode), error.output))
        raise


def _create_controller(path=None):
    return WindowsController(path) if platform.system() == "Windows" else UnixController(path)


def _parse_int(string_value, message):
    try:
        return int(string_value)
    except ValueError:
        raise RuntimeError("Int value expected for %s but received '%s'" % (message, string_value))


def _localhost(port):
    return "localhost:%d" % port

# todo:
#  - add read replicas dynamically
