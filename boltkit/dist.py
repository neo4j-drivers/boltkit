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


from os import getenv
import certifi
import json
import urllib3
from urllib3 import make_headers


DIST_HOST = "dist.neo4j.org"

TEAMCITY_HOST = "live.neo4j-build.io"
TEAMCITY_BUILD_CONFIG_ID = "Drivers_Neo4jArtifacts_Artifacts"
TEAMCITY_ARTIFACT_PATH = "neo4j-artifacts"
TEAMCITY_USER = getenv("TEAMCITY_USER")
TEAMCITY_PASSWORD = getenv("TEAMCITY_PASSWORD")


def byte_size_repr(b):
    scale = 0
    while b >= 1024:
        b /= 1024
        scale += 1
    if scale == 0:
        return "{}B".format(b)
    else:
        return "{:.1f}{}B".format(b, " KMGTP"[scale])


class Version(tuple):

    @classmethod
    def parse(cls, string):
        from unicodedata import category
        parts = []
        last_ch = None
        for ch in string:
            if last_ch is None:
                parts.append([ch])
            elif ch == ".":
                if last_ch in ".-":
                    parts[-1][-1] += "0"
                parts[-1].append("")
            elif ch == "-":
                if last_ch in ".-":
                    parts[-1][-1] += "0"
                parts.append([""])
            else:
                if last_ch not in ".-" and category(ch)[0] != category(last_ch)[0]:
                    parts.append([ch])
                else:
                    parts[-1][-1] += ch
            last_ch = ch
        for part in parts:
            for i, x in enumerate(part):
                try:
                    part[i] = int(x)
                except (ValueError, TypeError):
                    pass
            while len(part) > 1 and not part[-1]:
                part[:] = part[:-1]
        return cls(*map(tuple, parts))

    def __new__(cls, *parts):
        parts = list(parts)
        for i, part in enumerate(parts):
            if not isinstance(part, tuple):
                parts[i] = (part,)
        return super(Version, cls).__new__(cls, parts)

    def __repr__(self):
        return "%s%r" % (type(self).__name__, tuple(self))

    @property
    def primary(self):
        try:
            return self[0]
        except IndexError:
            return ()

    @property
    def secondary(self):
        try:
            return self[1]
        except IndexError:
            return ()


class Release:

    def __init__(self, name):
        self.name = name
        self.version = Version.parse(name)

    def __repr__(self):
        return "Release(%r)" % self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_prerelease(self):
        return bool(self.version.secondary)

    def package(self, edition, package_format):
        return Package(self, edition, package_format)


class Package:

    def __init__(self, release, edition, package_format):
        if package_format not in {"unix.tar.gz", "windows.zip"}:
            raise RuntimeError("Invalid package format %r" % package_format)
        self.release = release
        self.edition = edition
        self.package_format = package_format
        self.name = "neo4j-{}-{}-{}".format(edition, release.name, package_format)


class Distributor:

    def __init__(self):
        self.http = urllib3.PoolManager(
            cert_reqs="CERT_REQUIRED",
            ca_certs=certifi.where(),
            headers={"User-Agent": "neo4j-drivers/boltkit"})
        self._releases = None
        self.latest_release = None

    def refresh(self):
        """ Refresh the list of public Neo4j distributions.
        """
        r = self.http.request("GET", "https://api.github.com/repos/neo4j/neo4j/git/refs/tags")
        releases = {}
        latest_patches = {}
        latest_release = None
        for dist in (json.loads(r.data.decode("utf-8"))):
            ref = dist["ref"]
            if ref.startswith("refs/tags/"):
                release = Release(ref[10:])
                primary_version = release.version.primary
                is_numeric = all(isinstance(n, int) for n in primary_version)
                if is_numeric and 1 <= len(primary_version) <= 3 and primary_version >= (3,) and not release.is_prerelease():
                    releases[release.name.upper()] = release
                    try:
                        major, minor, patch = primary_version
                    except ValueError:
                        try:
                            major, minor = primary_version
                            patch = 0
                        except ValueError:
                            major, = primary_version
                            minor = 0
                            patch = 0
                    if (major, minor) in latest_patches:
                        latest_patches[(major, minor)] = max(latest_patches[(major, minor)], patch)
                    else:
                        latest_patches[(major, minor)] = patch
                    if latest_release is None:
                        latest_release = major, minor, patch
                    else:
                        latest_release = max(latest_release, (major, minor, patch))
        for (major, minor), patch in latest_patches.items():
            releases["{}.{}".format(major, minor)] = releases["{}.{}.{}".format(major, minor, patch)]
        if latest_release is not None:
            releases["LATEST"] = releases["{}.{}.{}".format(*latest_release)]
        self._releases = releases

    @property
    def releases(self):
        if self._releases is None:
            self.refresh()
        return self._releases

    def _download(self, url, path, auth=None, on_progress=None):
        if auth:
            headers = make_headers(basic_auth=":".join(auth))
        else:
            headers = None
        r = self.http.request("GET", url, headers=headers, preload_content=False)
        if r.status != 200:
            raise RuntimeError("Failed with status code %r" % r.status)
        try:
            content_length = int(r.getheader("Content-Length"))
        except (ValueError, TypeError):
            content_length = -1
        content_downloaded = 0
        with open(path, "wb") as out:
            while True:
                data = r.read(65536)
                if not data:
                    break
                out.write(data)
                content_downloaded += len(data)
                if callable(on_progress):
                    on_progress(content_downloaded, content_length)
        r.release_conn()

    @classmethod
    def _print_progress(cls, progress, total):
        percentage_downloaded = int(100 * progress / total)
        progress_string = "{:>3}% [{:<50}] {} of {}".format(percentage_downloaded,
                                                            "=" * (percentage_downloaded // 2),
                                                            byte_size_repr(progress),
                                                            byte_size_repr(total))
        print("{:<80}".format(progress_string), end="\r\n" if percentage_downloaded == 100 else "\r")

    def download(self, edition, version, package_format):
        """ Download a public release.
        """
        try:
            release = self.releases[version.upper()]
        except KeyError:
            raise ValueError("No release available with version {}".format(version))
        package = release.package(edition, package_format)
        dist_host = getenv("DIST_HOST", DIST_HOST)
        download_url = "https://{}/{}".format(dist_host, package.name)
        print("Downloading {} from {}".format(package.name, dist_host))
        self._download(download_url, package.name, on_progress=self._print_progress)

    def download_from_s3(self, edition, version, package_format):
        """ TODO """

    def download_from_teamcity(self, edition, version, package_format):
        release = Release(version)
        package = release.package(edition, package_format)
        download_url = "https://{}/repository/download/{}/lastSuccessful/{}/{}".format(TEAMCITY_HOST, TEAMCITY_BUILD_CONFIG_ID, TEAMCITY_ARTIFACT_PATH, package.name)
        if TEAMCITY_USER is None or TEAMCITY_PASSWORD is None:
            raise RuntimeError("No auth details found in TEAMCITY_USER and TEAMCITY_PASSWORD")
        print("Downloading {} from TeamCity".format(package.name))
        self._download(download_url, package.name, auth=(TEAMCITY_USER, TEAMCITY_PASSWORD),
                       on_progress=self._print_progress)
