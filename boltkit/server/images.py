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


from logging import getLogger
from os import getenv
from xml.etree import ElementTree

import certifi
from docker import DockerClient
from docker.errors import ImageNotFound
from urllib3 import PoolManager, make_headers


log = getLogger("boltkit")


snapshot_host = "live.neo4j-build.io"
snapshot_build_config_id = "Neo4j40_Docker"
snapshot_build_url = ("https://{}/repository/download/{}/"
                      "lastSuccessful".format(snapshot_host,
                                              snapshot_build_config_id))


teamcity_http = PoolManager(
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    headers=make_headers(basic_auth="{}:{}".format(
        getenv("TEAMCITY_USER", ""),
        getenv("TEAMCITY_PASSWORD", ""),
    )),
)


docker = DockerClient.from_env(version="auto")


def resolve_image(image):
    """ Resolve an informal image tag into a full Docker image tag. Any tag
    available on Docker Hub for Neo4j can be used, and if no 'neo4j:' prefix
    exists, this will be added automatically. The default edition is
    Community, unless a cluster is being created in which case Enterprise
    edition is selected instead. Explicit selection of Enterprise edition can
    be made by adding an '-enterprise' suffix to the image tag.

    The pseudo-tag 'snapshot' will download the latest bleeding edge image
    from TeamCity. Note that this is a large download (600-700MB) and requires
    credentials to be passed through the TEAMCITY_USER and TEAMCITY_PASSWORD
    environment variables. Adding a '!' character after 'snapshot' will force
    a download, avoiding any local caching issues.

    If a 'file:' URI is passed in here instead of an image tag, the Docker
    image will be loaded from that file instead.

    Examples of valid tags:
    - 3.4.6
    - neo4j:3.4.6
    - latest
    - snapshot
    - snapshot!
    - file:/home/me/image.tar

    """
    resolved = image
    if resolved.startswith("file:"):
        return load_image_from_file(resolved[5:])
    if ":" not in resolved:
        resolved = "neo4j:" + image
    if resolved.endswith("!"):
        force = True
        resolved = resolved[:-1]
    else:
        force = False
    if resolved == "neo4j:snapshot":
        return pull_snapshot("community", force)
    elif resolved in ("neo4j:snapshot-enterprise",
                      "neo4j-enterprise:snapshot"):
        return pull_snapshot("enterprise", force)
    else:
        return resolved


def load_image_from_file(name):
    with open(name, "rb") as f:
        images = docker.images.load(f.read())
        image = images[0]
        return image.tags[0]


def pull_snapshot(edition, force):
    """ Ensure a local copy of the snapshot image is available. If 'force' is
    True, then a download will always happen, regardless of the local cache.
    """
    artifact = resolve_artifact_name(edition)
    if force:
        return download_snapshot_artifact(artifact)
    else:
        derived = derive_image_tag(artifact)
        try:
            docker.images.get(derived)
        except ImageNotFound:
            return download_snapshot_artifact(artifact)
        else:
            return derived


def resolve_artifact_name(edition):
    log.info("Resolving snapshot artifact name on «{}»".format(
        snapshot_host))
    prefix = "neo4j-{}".format(edition)
    url = "{}/teamcity-ivy.xml".format(snapshot_build_url)
    log.debug("Fetching build data from {}".format(url))
    r1 = teamcity_http.request("GET", url)
    if r1.status != 200:
        raise RuntimeError("Download failed ({})".format(r1.status))
    root = ElementTree.fromstring(r1.data)
    for e in root.find("publications").findall("artifact"):
        attr = e.attrib
        if attr["type"] == "tar" and attr["name"].startswith(prefix):
            return "{}.{}".format(attr["name"], attr["ext"])
    raise ValueError("No artifact found for {} edition".format(edition))


def derive_image_tag(artifact_name):
    if artifact_name.endswith("-docker-loadable.tar"):
        artifact_name = artifact_name[:-20]
    else:
        raise ValueError("Expected artifact name to end with "
                         "'-docker-loadable.tar'")
    if artifact_name.startswith("neo4j-enterprise-"):
        return "neo4j-enterprise:{}".format(artifact_name[17:])
    elif artifact_name.startswith("neo4j-community-"):
        return "neo4j:{}".format(artifact_name[16:])
    else:
        raise ValueError("Expected artifact name to start with either "
                         "'neo4j-community-' or 'neo4j-enterprise-'")


def download_snapshot_artifact(artifact):
    log.info("Downloading {} from «{}»".format(
        artifact, snapshot_host))
    url = "{}/{}".format(snapshot_build_url, artifact)
    log.debug("Downloading file {}".format(url))
    r2 = teamcity_http.request("GET", url)
    images = docker.images.load(r2.data)
    image = images[0]
    return image.tags[0]
