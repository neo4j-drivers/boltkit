from os.path import join as path_join, realpath
from boltkit.cluster import Cluster, create_sub_commands_description, DEFAULT_INITIAL_PORT
from argparse import ArgumentParser, RawDescriptionHelpFormatter
import sys
import json

class MultiCluster:

    def __init__(self, path):
        self.clusters = {}
        self.path = path

    # param databases: pairs of db_name=(version, core_count, read_replica_count, initial_port, password, verbose)
    def install(self, **database):
        multicluster_initial_discovery_members = []
        for name in database:
            cluster = Cluster(path_join(self.path, name))
            self.clusters[name] = cluster
            cluster.install(*database[name])
            multicluster_initial_discovery_members.append(cluster.initial_discovery_members)

        for name in self.clusters:
            properties = {
                "causal_clustering.initial_discovery_members": multicluster_initial_discovery_members,
                "causal_clustering.database": name
            }
            self.clusters[name].update_config(properties)
        return realpath(self.path)

    # param timeout: timeout for starting each cluster in multi-cluster
    def start(self, timeout):
        members = ""
        for cluster in self.clusters:
            members += cluster.start(timeout)
        return members

    def stop(self, kill):
        for cluster in self.clusters:
            cluster.kill(kill)


def _execute_cluster_command(parsed):
    command = parsed.command
    cluster_ctrl = MultiCluster(parsed.path)
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
            "name": name,
            "core_count": core_count,
            "read_replica_count": read_replica_count,
            "initial_port": initial_port,
            "password": parsed.password,
            "verbose": parsed.verbose
        }
        database[name] = args
    return database


def multicluster():
    parsed = parse_args(sys.argv[1:])
    _execute_cluster_command(parsed)


def parse_args(args):
    see_download_command = ("See neoctrl-download for details of supported environment variables.\r\n"
                            "\r\n"
                            "Report bugs to drivers@neo4j.com")

    parser = ArgumentParser(description="Operate Neo4j multi-cluster.\r\n",
                            epilog=see_download_command,
                            formatter_class=RawDescriptionHelpFormatter)

    sub_commands_with_description = {
        "install": "Download, extract and configure multi-cluster",
        "start": "Start the multi-cluster located at the given path",
        "stop": "Stop the multi-cluster located at the given path"
    }

    subparsers = parser.add_subparsers(title="available sub-commands", dest="command",
                                       help="commands are available",
                                       description=create_sub_commands_description(sub_commands_with_description))

    # install
    parser_install = subparsers.add_parser("install", epilog=see_download_command,
                                           description=
                                           sub_commands_with_description["install"] +
                                           "\r\n\r\nexample:\r\n"
                                           "  neoctrl-multicluster install 3.4.0 $HOME/multi-cluster/ -p pAssw0rd [-v]"
                                           " -d '{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}'",
                                           formatter_class=RawDescriptionHelpFormatter)

    parser_install.add_argument("version", help="Neo4j server version")
    parser_install.add_argument("path", nargs="?", default=".", help="download destination path (default: .)")
    parser_install.add_argument("-p", "--password", required=True,
                                help="initial password of the initial admin user ('neo4j') for all cluster members")
    parser_install.add_argument("-v", "--verbose", action="store_true", help="show more detailed output")
    parser_install.add_argument("-d","--database", dest="database", required=True,
                                help="A json string describes the multi-cluster structure. "
                                     "e.g. '{\"london\": {\"c\": 3, \"r\": 2}, \"malmo\": {\"c\": 5, \"i\": 9001}}'"
                                     "c: core_size=3, r: read_replia_size=0, i: initial_port=%d" % DEFAULT_INITIAL_PORT)

    # start
    parser_start = subparsers.add_parser("start", epilog=see_download_command,
                                         description=
                                         sub_commands_with_description["start"] +
                                         "\r\n\r\nexample:\r\n"
                                         "  neoctrl-multicluster start $HOME/cluster/",
                                         formatter_class=RawDescriptionHelpFormatter)

    parser_start.add_argument("-t", "--timeout", default=180, dest="timeout", type=int,
                              help="startup timeout for each cluster inside the multicluster in seconds (default: 180)")
    parser_start.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    # stop
    parser_stop = subparsers.add_parser("stop", epilog=see_download_command,
                                        description=
                                        sub_commands_with_description["stop"] +
                                        "\r\n\r\nexample:\r\n"
                                        "  neoctrl-multicluster stop $HOME/cluster/",
                                        formatter_class=RawDescriptionHelpFormatter)

    parser_stop.add_argument("-k", "--kill", action="store_true",
                             help="forcefully kill all instances in the cluster")
    parser_stop.add_argument("path", nargs="?", default=".", help="causal cluster location path (default: .)")

    return parser.parse_args(args)