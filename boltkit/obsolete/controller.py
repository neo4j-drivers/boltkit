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
from os import getenv, makedirs
from os.path import join as path_join, normpath, realpath, isfile, getsize
from random import randint
from socket import create_connection
from subprocess import call, check_output, CalledProcessError
from sys import stderr, stdin
from time import sleep

import boto
from boto.s3.key import Key
from boto.s3.connection import OrdinaryCallingFormat

import boltkit.config as config

try:
    from urllib.request import urlopen, Request, HTTPError
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen, Request, HTTPError
    from urlparse import urlparse

from OpenSSL import crypto, SSL
from socket import gethostname

DIST_HOST = "dist.neo4j.org"

CERT_FOLDER = "certificates"  # Relative to neo4j_home
CERT_FILE = "neo4j.cert"
KEY_FILE = "neo4j.key"


def _for_40_server():
    return {
        "dbms.ssl.policy.bolt.enabled": "true",
        "dbms.ssl.policy.bolt.base_directory": CERT_FOLDER,
        "dbms.ssl.policy.bolt.private_key": KEY_FILE,
        "dbms.ssl.policy.bolt.public_certificate": CERT_FILE,
        "dbms.ssl.policy.bolt.client_auth": "none",
    }


def bstr(s):
    return s if isinstance(s, (bytes, bytearray)) else s.encode("UTF-8")


class Downloader(object):

    def __init__(self, path, verbose=False):
        self.path = path
        self.verbose = verbose

    def write(self, message):
        if self.verbose:
            stderr.write(message)

    def download_nightly_build(self, major, minor, edition, package_format):
        package = "neo4j-%s-%s.%s-NIGHTLY-%s" % (edition, major, minor, package_format)
        package = self.download_build(package)
        self.write("\r\n")
        return package

    def download_build(self, package):
        """ Download from TeamCity build server.
        """
        url = get_env_variable_or_raise_error("TEAMCITY_HOST") + "/" + package
        user = get_env_variable_or_raise_error("TEAMCITY_USER")
        password = get_env_variable_or_raise_error("TEAMCITY_PASSWORD")
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

    def download_s3(self, package):
        """ Download from private s3 distributions.
        """
        package_path = path_join(self.path, package)
        aws_access_key_id = get_env_variable_or_raise_error("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = get_env_variable_or_raise_error("AWS_SECRET_ACCESS_KEY")

        bucket_name = getenv("BUCKET", DIST_HOST)
        # connect to the bucket
        conn = boto.s3.connect_to_region(
            "eu-west-1",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            is_secure=True,
            calling_format=OrdinaryCallingFormat()
        )
        bucket = conn.get_bucket(bucket_name)
        # Get the Key object of the given key, in the bucket
        k = Key(bucket, package)

        # Ensure the destination exist
        try:
            makedirs(self.path)
        except OSError:
            pass
        self.write("Downloading from aws bucket %s... " % bucket_name)

        # Get the contents of the key into a file
        k.get_contents_to_filename(package_path)
        return package_path

    def download_dist(self, package):
        """ Download from public distributions.
        """
        dist_address = getenv("DIST_HOST", DIST_HOST)
        url = "http://%s/%s" % (dist_address, package)
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
        existing_package = getenv("NEOCTRL_LOCAL_PACKAGE", None)
        if existing_package is not None:
            if isfile(existing_package):
                return existing_package
            raise RuntimeError("Unable to locate existing package at %s" % existing_package)

        version_parts = version.replace("-", ".").replace("_", ".").split(".")

        if len(version_parts) == 2:
            major, minor = version_parts
            return self.download_nightly_build(major, minor, edition, package_format)
        elif len(version_parts) == 3:
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

        package = self.download_s3(package) if edition == "enterprise" and len(version_parts) != 3 else self.download_dist(package)
        self.write("\r\n")

        return package


def wait_for_server(host, port, timeout=0):
    port = port or 7474
    running = False
    t = 0
    while not running and timeout and t < timeout:
        try:
            s = create_connection((host, port))
        except IOError:
            sleep(1)
            t += 1
        else:
            s.close()
            running = True

    if not running:
        raise RuntimeError("Server %s:%d did not become available in %r seconds" % (host, port, timeout))


def hex_bytes_str(data):
    return "".join("%02X" % b for b in bytearray(data))


def user_record(user, password):
    salt = bytearray(randint(0x00, 0xFF) for _ in range(16))
    m = sha256()
    m.update(salt)
    m.update(bstr(password))
    return b"".join([bstr(user), b":SHA-256,", bstr(hex_bytes_str(m.digest())), b",", bstr(hex_bytes_str(salt)), b":"])


def get_env_variable_or_raise_error(name):
    value = getenv(name)
    if value is None:
        raise TypeError("Required environment variable is not defined: %s" % name)
    return value


def create_self_signed_cert(cert_path, key_path):
    # create a key pair
    key = crypto.PKey()
    key.generate_key(crypto.TYPE_RSA, 1024)

    # create a self-signed cert
    cert = crypto.X509()
    cert.get_subject().C = "UK"
    cert.get_subject().ST = "London"
    cert.get_subject().L = "London"
    cert.get_subject().O = "Dev"
    cert.get_subject().OU = "Drivers"
    cert.get_subject().CN = "localhost"
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(10*365*24*60*60)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(key)
    cert.sign(key, 'SHA512')

    with open(cert_path, "wb") as cert_file:
        cert_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
    with open(key_path, "wb") as key_file:
        key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))


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
        properties = config.common_config()
        if version.startswith("4."):
            # Install a self-signed cert before server start.
            try:
                makedirs(path_join(home, CERT_FOLDER))
            except OSError:
                pass
            create_self_signed_cert(path_join(home, CERT_FOLDER, CERT_FILE),
                                    path_join(home, CERT_FOLDER, KEY_FILE))
            properties.update(_for_40_server())
        config.update(home, properties)
        return realpath(home)

    @classmethod
    def os_dependent_config(cls, instance_id):
        raise NotImplementedError("Not yet supported for this platform")

    def __init__(self, home, verbosity=0):
        self.home = home
        self.verbosity = verbosity

    def start(self, timeout=0):
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
            live_users_detected = b"password was not set" in output.lower()
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
            return b"set-initial-password" in output
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

    def start(self, timeout=0):
        http_uri, bolt_uri = config.extract_http_and_bolt_uris(self.home)
        _invoke([path_join(self.home, "bin", "neo4j"), "start"])
        if timeout:
            try:
                wait_for_server(http_uri.hostname, http_uri.port, timeout=timeout)
            except RuntimeError:
                with open(path_join(self.home, "logs", "neo4j.log"), "r") as log_file:
                    print(log_file.read())
                raise
        return InstanceInfo(http_uri, bolt_uri, self.home)

    def stop(self, kill=False):
        if kill:
            output = _invoke([path_join(self.home, "bin", "neo4j"), "status"]).decode("utf-8")
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
            # take first entry form zip file, it should be something like `neo4j-enterprise-3.0.0/plugins`
            firstName = files.namelist()[0]
            # strip off everything except the root path, it should become something like `neo4j-enterprise-3.0.0`
            rootPath = firstName[0:firstName.find("/")]  # ZIP file system always has "/" as separator
            return path_join(path, rootPath)

    @classmethod
    def os_dependent_config(cls, instance_id):
        return {config.WINDOWS_SERVICE_NAME_SETTING: instance_id}

    def start(self, timeout=0):
        http_uri, bolt_uri = config.extract_http_and_bolt_uris(self.home)
        _invoke([path_join(self.home, "bin", "neo4j.bat"), "install-service"])
        _invoke([path_join(self.home, "bin", "neo4j.bat"), "start"])
        if timeout:
            wait_for_server(http_uri.hostname, http_uri.port, timeout=timeout)
        return InstanceInfo(http_uri, bolt_uri, self.home)

    def stop(self, kill=False):
        if kill:
            windows_service_name = config.extract_windows_service_name(self.home)
            sc_output = _invoke(["sc", "queryex", windows_service_name]).decode("utf-8")
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
               "  DIST_HOST         name of distribution server (default: dist.neo4j.org)"
               "  TEAMCITY_HOST     name of build server\r\n"
               "  TEAMCITY_USER     build server user name\r\n"
               "  TEAMCITY_PASSWORD build server password\r\n"
               "  AWS_ACCESS_KEY_ID aws access key id\r\n"
               "  AWS_SECRET_ACCESS_KEY aws secret access key\r\n"
               "\r\n"
               "TEAMCITY_* environment variables are required to download snapshot servers.\r\n"
               "AWS_* environment variables are used to access enterprise distribution servers.\r\n"
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
    edition = "enterprise" if parsed.enterprise else "community"
    controller = create_controller()
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
    controller = create_controller()
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
    parser.add_argument("-t", "--timeout", type=float, default=90,
                        help="number of seconds to wait for server to start")
    parser.add_argument("home", nargs="?", default=".", help="Neo4j server directory (default: .)")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        controller = WindowsController(parsed.home, 1 if parsed.verbose else 0)
    else:
        controller = UnixController(parsed.home, 1 if parsed.verbose else 0)
    instance_info = controller.start(timeout=parsed.timeout)

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
    controller = create_controller(parsed.home)
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
    controller.create_user(parsed.user, parsed.password)


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
            controller.start(timeout=300)
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


def create_controller(path=None):
    return WindowsController(path) if platform.system() == "Windows" else UnixController(path)
