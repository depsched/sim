import argparse
import os
import sys
import socket
import subprocess

official_images = "adminer aerospike alpine amazonlinux arangodb " \
                  "backdrop bash bonita buildpack busybox cassandra " \
                  "centos chronograf cirros clearlinux clefos clojure " \
                  "composer consul convertigo couchbase couchdb crate " \
                  "crux debian docker drupal eclipse eggdrop elasticsearch" \
                  "elixir erlang euleros fedora flink fsharp gazebo gcc " \
                  "geonetwork ghost golang gradle groovy haproxy haskell " \
                  "haxe hello hello hola httpd hylang ibmjava influxdb " \
                  "irssi jenkins jetty joomla jruby julia kaazing kapacitor " \
                  "kibana known kong lightstreamer logstash mageia mariadb maven " \
                  "mediawiki memcached mongo mongo mono mysql nats nats neo4j " \
                  "neurodebian nextcloud nginx node notary nuxeo odoo open " \
                  "openjdk opensuse oraclelinux orientdb owncloud percona " \
                  "perl photon php php piwik plone postgres pypy python r " \
                  "rabbitmq rakudo rapidoid redis redmine registry rethinkdb " \
                  "rocket ros ruby rust sentry silverpeas sl solr sonarqube " \
                  "sourcemage spiped storm swarm swift swipl teamspeak telegraf " \
                  "thrift tomcat tomee traefik ubuntu vault websphere wordpress " \
                  "xwiki znc zookeeper".split()

ecr_official_images = ['adminer', 'aerospike', 'alpine', 'amazonlinux', 'arangodb', 'backdrop',
                       'bash', 'bonita', 'busybox', 'cassandra', 'centos', 'chronograf', 'cirros',
                       'clojure', 'composer', 'consul', 'convertigo', 'couchbase', 'couchdb',
                       'crate', 'crux', 'debian', 'docker', 'drupal', 'eggdrop', 'erlang',
                       'fedora', 'flink', 'fsharp', 'gazebo', 'gcc', 'geonetwork', 'ghost',
                       'golang', 'gradle', 'groovy', 'haproxy', 'haskell', 'haxe', 'hello',
                       'hello', 'httpd', 'hylang', 'ibmjava', 'influxdb', 'irssi', 'jenkins',
                       'jetty', 'joomla', 'jruby', 'julia', 'kapacitor', 'kibana', 'known',
                       'kong', 'lightstreamer', 'logstash', 'mageia', 'mariadb', 'maven',
                       'mediawiki', 'memcached', 'mongo', 'mongo', 'mono', 'mysql', 'nats',
                       'nats', 'neo4j', 'neurodebian', 'nextcloud', 'nginx', 'node', 'nuxeo',
                       'odoo', 'openjdk', 'opensuse', 'oraclelinux', 'orientdb', 'owncloud',
                       'percona', 'perl', 'photon', 'php', 'php', 'piwik', 'plone', 'postgres',
                       'pypy', 'python', 'rabbitmq', 'rapidoid', 'redis', 'redmine', 'registry',
                       'rethinkdb', 'ros', 'ruby', 'rust', 'sentry', 'solr', 'sonarqube', 'sourcemage',
                       'spiped', 'storm', 'swarm', 'swift', 'teamspeak', 'telegraf', 'thrift', 'tomcat',
                       'tomee', 'traefik', 'ubuntu', 'vault', 'wordpress', 'xwiki', 'znc', 'zookeeper']


def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


def main_with_cmds(cmds={}, add_arguments=None):
    def _print_usage(parser):
        parser.print_help(file=sys.stderr)
        sys.exit(2)

    parser = argparse.ArgumentParser(description='Collector cmds.')

    cmds.update({
        'argtest': lambda: print("halo, arg arg."),
        'help': lambda: _print_usage(parser),
    })

    for name in list(cmds.keys()):
        if '_' in name:
            cmds[name.replace('_', '-')] = cmds[name]

    cmdlist = sorted(cmds.keys())

    parser.add_argument(
        'action',
        metavar='action',
        nargs='?',
        default='help',
        choices=cmdlist,
        help='Action is one of ' + ', '.join(cmdlist))

    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='enable verbose')

    if add_arguments:
        add_arguments(parser)

    args = parser.parse_args()

    if args.verbose:
        os.environ['V'] = '1'
    cmds[args.action]()


def depsched_intro():
    """Print to the stdout the program banner."""
    print("""
       .___                         .__               .___
     __| _/____ ______  ______ ____ |  |__   ____   __| _/
    / __ |/ __ \\\\____ \/  ___// ___\|  |  \_/ __ \ / __ |
   / /_/ \  ___/|  |_> >___ \\\\ \___ |   Y  \  ___// /_/ |
   \____ |\___  >   __/____  >\___  >___|  /\___  >____ |
        \/    \/|__|       \/     \/     \/     \/     \/ """)


def cmd(cmd, quiet=False):
    """Executes a subprocess running a shell command and returns the output."""
    if quiet:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True)
    else:
        proc = subprocess.Popen(cmd, shell=True)

    out, _ = proc.communicate()

    if proc.returncode:
        if quiet:
            print('Log:\n', out, file=sys.stderr)
        print('Error has occurred running command: %s' % cmd, file=sys.stderr)
        print("move on.")
    return out
