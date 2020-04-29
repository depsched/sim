import subprocess
import sys


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
    return out


if __name__ == "__main__":
    images = "adminer aerospike alpine amazonlinux arangodb " \
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
    print("pulling {} images".format(len(images)))
    counter = 0
    for image in images:
        try:
            cmd("docker pull {}".format(image))
            counter += 1
        except:
            pass
    print("done; pulled {} images".format(counter))

