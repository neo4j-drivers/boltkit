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

from argparse import ArgumentParser
from base64 import b64encode
from hashlib import sha256
from json import dumps as json_dumps, loads as json_loads
from os import getenv, makedirs
from os.path import join as path_join, normpath
import platform
from random import randint
from shutil import rmtree
from socket import create_connection
from subprocess import check_output
from sys import stderr, stdin
from tempfile import mkdtemp
from time import sleep
try:
    from urllib.request import urlopen, Request, HTTPError
    from urllib.parse import urlparse
except ImportError:
    from urllib2 import urlopen, Request, HTTPError
    from urlparse import urlparse


NEO4J_DIST_ADDRESS = "dist.neo4j.org"


class Downloader(object):

    def __init__(self, path, verbose=False):
        self.path = path
        self.verbose = verbose

    def write(self, message):
        if self.verbose:
            stderr.write(message)

    def download_build(self, address, build_id, package, user, password):
        """ Download from TeamCity build server.
        """
        url = "https://%s/repository/download/%s/.lastSuccessful/%s" % (address, build_id, package)
        auth_token = b"Basic " + b64encode(("%s:%s" % (user, password)).encode("iso-8859-1"))
        request = Request(url, headers={"Authorization": auth_token})
        package_path = path_join(self.path, package)

        self.write("Downloading build from %s... " % url)
        fin = urlopen(request)
        try:
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
        elif len(version_parts) == 4:
            major, minor, patch, milestone = version_parts
        else:
            raise ValueError("Unrecognised version %r" % version)

        # Derive template and package version
        if package_format in ("unix.tar.gz", "windows.zip"):
            template = "neo4j-%s-%s-%s"
            if milestone:
                package_version = "%s.%s.%s-%s" % (major, minor, patch, milestone)
            else:
                package_version = "%s.%s.%s" % (major, minor, patch)
        else:
            raise ValueError("Unknown format %r" % package_format)
        package = template % (edition, package_version, package_format)

        dist_address = getenv("NEO4J_DIST_ADDRESS", NEO4J_DIST_ADDRESS)
        build_address = getenv("NEO4J_BUILD_ADDRESS")
        if build_address:
            try:
                build_id = "Neo4j%s%s_Packaging" % (major, minor)
                build_user = getenv("NEO4J_BUILD_USER")
                build_password = getenv("NEO4J_BUILD_PASSWORD")
                package = self.download_build(build_address, build_id, package,
                                              build_user, build_password)
            except HTTPError:
                package = self.download_dist(dist_address, package)
        else:
            package = self.download_dist(dist_address, package)
        self.write("\r\n")

        return package


def untar(archive, path):
    from tarfile import TarFile
    with TarFile.open(archive) as files:
        files.extractall(path)
        return path_join(path, files.getnames()[0])


def unzip(archive, path):
    from zipfile import ZipFile
    with ZipFile(archive, 'r') as files:
        files.extractall(path)
        return path_join(path, files.namelist()[0])


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


class Unix(object):

    @classmethod
    def download(cls, edition, version, work_dir, **kwargs):
        work_dir = mkdtemp() if work_dir is None else normpath(work_dir)
        downloader = Downloader(work_dir, verbose=kwargs.get("verbose"))
        package = downloader.download(edition, version, "unix.tar.gz")
        return package

    @classmethod
    def install(cls, edition, version, work_dir, **kwargs):
        package = cls.download(edition, version, work_dir, **kwargs)
        home = untar(package, work_dir)
        return home

    @classmethod
    def uninstall(cls, home, **kwargs):
        rmtree(normpath(path_join(home, "..")))

    @classmethod
    def start(cls, home, **kwargs):
        out = check_output([path_join(home, "bin", "neo4j"), "start"])
        uri = None
        parsed_uri = None
        for line in out.splitlines():
            http = line.find(b"http:")
            if http >= 0:
                uri = line[http:].split()[0].strip()
                parsed_uri = urlparse(uri)
        if not parsed_uri:
            raise RuntimeError("Cannot ascertain server address")
        wait_for_server(parsed_uri.hostname, parsed_uri.port)
        return uri.decode("iso-8859-1")

    @classmethod
    def stop(cls, home, **kwargs):
        check_output([path_join(home, "bin", "neo4j"), "stop"])

    @classmethod
    def create_user(cls, home, user, password, **kwargs):
        data_dbms = path_join(home, "data", "dbms")
        try:
            makedirs(data_dbms)
        except OSError:
            pass
        with open(path_join(data_dbms, "auth"), "a") as f:
            f.write(user_record(user, password))
            f.write(b"\r\n")


class Windows(object):

    @classmethod
    def download(cls, edition, version, work_dir, **kwargs):
        work_dir = mkdtemp() if work_dir is None else normpath(work_dir)
        downloader = Downloader(work_dir, verbose=kwargs.get("verbose"))
        package = downloader.download(edition, version, "windows.zip")
        return package

    @classmethod
    def install(cls, edition, version, work_dir, **kwargs):
        package = cls.download(edition, version, work_dir, **kwargs)
        home = unzip(package, work_dir)
        return home

    @classmethod
    def uninstall(cls, home, **kwargs):
        rmtree(normpath(path_join(home, "..")))

    @classmethod
    def start(cls, home, **kwargs):
        raise NotImplementedError("Windows support not complete")

    @classmethod
    def stop(cls, home, verbose=False):
        raise NotImplementedError("Windows support not complete")

    @classmethod
    def create_user(cls, home, user, password, **kwargs):
        raise NotImplementedError("Windows support not complete")


def usage():
    print("usage: neotest-download <version> <work_dir>")
    print("       neotest-install <version> <work_dir>")
    print("       neotest-uninstall <home>")
    print("       neotest-start <home>")
    print("       neotest-stop <home>")
    print("       neotest-create-user <home> <user> <password>")


def download():
    parser = ArgumentParser()
    parser.add_argument("-e", "--enterprise", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("version")
    parser.add_argument("work_dir", default=None)
    parsed = parser.parse_args()
    edition = "enterprise" if parsed.enterprise else "community"
    try:
        if platform.system() == "Windows":
            home = Windows.download(edition, parsed.version, parsed.work_dir, verbose=parsed.verbose)
        else:
            home = Unix.download(edition, parsed.version, parsed.work_dir, verbose=parsed.verbose)
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


def install():
    parser = ArgumentParser()
    parser.add_argument("-e", "--enterprise", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("version")
    parser.add_argument("work_dir", default=None)
    parsed = parser.parse_args()
    edition = "enterprise" if parsed.enterprise else "community"
    try:
        if platform.system() == "Windows":
            home = Windows.install(edition, parsed.version, parsed.work_dir, verbose=parsed.verbose)
        else:
            home = Unix.install(edition, parsed.version, parsed.work_dir, verbose=parsed.verbose)
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


def uninstall():
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("home")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        Windows.uninstall(parsed.home, verbose=parsed.verbose)
    else:
        Unix.uninstall(parsed.home, verbose=parsed.verbose)


def start():
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("home")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        http_uri = Windows.start(parsed.home, verbose=parsed.verbose)
    else:
        http_uri = Unix.start(parsed.home, verbose=parsed.verbose)
    if http_uri:
        f = urlopen(http_uri)
        try:
            data = f.read()
        finally:
            f.close()
        uris = json_loads(data.decode("utf-8"))
        bolt_uri = uris["bolt"]
        print("%s %s" % (http_uri, bolt_uri))


def stop():
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("home")
    parsed = parser.parse_args()
    if platform.system() == "Windows":
        Windows.stop(parsed.home, verbose=parsed.verbose)
    else:
        Unix.stop(parsed.home, verbose=parsed.verbose)


def create_user():
    parser = ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("home")
    parser.add_argument("user")
    parser.add_argument("password")
    parsed = parser.parse_args()
    user = parsed.user.encode(stdin.encoding or "utf-8")
    password = parsed.password.encode(stdin.encoding or "utf-8")
    if platform.system() == "Windows":
        Windows.create_user(parsed.home, user, password, verbose=parsed.verbose)
    else:
        Unix.create_user(parsed.home, user, password, verbose=parsed.verbose)
