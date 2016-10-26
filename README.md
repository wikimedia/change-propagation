# Change Propagation 
[![Version](https://img.shields.io/npm/v/change-propagation.svg?maxAge=2592000&style=flat-square)](https://www.npmjs.com/package/change-propagation)
[![Travis](https://img.shields.io/travis/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://travis-ci.org/wikimedia/change-propagation)
[![Coveralls](https://img.shields.io/coveralls/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://coveralls.io/github/wikimedia/change-propagation)
[![Dependencies](https://img.shields.io/david/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://david-dm.org/wikimedia/change-propagation)
[![License](https://img.shields.io/github/license/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://github.com/wikimedia/change-propagation/blob/master/LICENSE)

A [RESTBase](https://github.com/wikimedia/restbase) queuing module for
[Apache Kafka](http://kafka.apache.org/)

The purpose of the change propagation service is executing actions based on events. The service
listens to kafka topics, and executes handlers for events according to configurable rules. Currently,
a rule could issue HTTP requests, produce new messages, or make an HTCP purge request. The list of
supported actions is easily expandable by creating new modules with internal HTTP endpoints and
calling them from the rules.

## Features

- Config-based rules for message processing. For more information about rules configuration
see [Configuration](##Rule configuration) section.
- Automatic limited retries
- Global rule execution concurrency limiting
- Metrics and logging support

## Rule Configuration

A `Rule` is a semantically meaningful piece of service functionality. For example,
'Rerender RESTBase if the page was changed', or 'Update summary if RESTBase render was changed'
are both rules. To specify the rules, you need to add a property to the `kafka` module config
[template property](https://github.com/wikimedia/change-propagation/blob/master/config.example.yaml#L48).
Each rule is executed by a single worker, but internal load-balancing mechanism tries to distribute
rules to workers equally.

The rule can contain the following properties:
- **topic** A name of the topic to subscribe to.
- **match** An optional predicate for a message. The rule is executed only if all of the `match`
properties were satisfied by the message. Properties could be nested objects, constants
or a regex. Regex could contain capture groups and captured values will later be accessible
in the `exec` part of the rule. Capture groups could be named, using the `(?<name>group)` syntax, then
the captured value would be accessible under `match.property_name.capture_name` within the `exec` part.
Named and unnamed captures can not be mixed together.
- **match_not** An optional predicate which must not match for a rule to be executed. It doesn't capture values
and doesn't make them accessible to the `exec` part of the rule. The `match_not` may be an array with the semantics
of logical OR - if any of the array items match, the `match_not` matches.
- **exec** An array of HTTP request templates, that will be executed sequentially if the rule matched.
The template follows [request templating syntax](https://github.com/wikimedia/swagger-router#request-templating).
The template is evaluated with a `context` that has `message` global property with an original message,
and `match` property with values extracted by the match.

Here's an example of the rule, which would match all `resource_change` messages, emitted by `RESTBase`,
and purge varnish caches for the resources by issuing an HTTP request to a special internal module, that would
convert it to HTCP purge and make an HTCP request:
```yaml
    purge_varnish:
      topic: resource_change
      match:
        meta:
          uri: '/^https?:\/\/[^\/]+\/api\/rest_v1\/(?<rest>.+)$/'
        tags:
          - restbase
      exec:
        method: post
        uri: '/sys/purge/'
        body:
          - meta:
              uri: '//{{message.meta.domain}}/api/rest_v1/{{match.meta.uri.rest}}'

```


## Testing

For testing locally you need to setup and start Apache Kafka and set the 
`KAFKA_HOME` environment variable to point to the Kafka home directory.
Here's a sample script you need to run:

```bash
export KAFKA_HOME=<your desired kafka install path>
wget http://www.us.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz -O kafka.tgz
mkdir -p $KAFKA_HOME && tar xzf kafka.tgz -C $KAFKA_HOME --strip-components 1
echo "KAFKA_HOME=$KAFKA_HOME" >> ~/.bash_profile
echo "PATH=\$PATH:\$KAFKA_HOME/bin" >> ~/.bash_profile
```

Also, you need to enable topic deletion so that the test scripts could clean up
kafka state before each test run:

```bash
echo 'delete.topic.enable=true' >> KAFKA_HOME/config/server.properties
```

Before starting the development version of change propagation or running
test you need to start Zookeeper and Kafka with `start-kafka` npm script.
To stop Kafka and Zookeeper tun `stop-kafka` npm script.

## Running locally

To run the service locally, you need to have to have kafka and zookeeper installed
and run. Example of installation and configuration can be found in the [Testing](##Testing)
section of this readme. After kafka is installed, configured, and run with `npm run start-kafka`
command, copy the example config and run the service:
```bash
cp config.example.yaml config.yaml
npm start
```

Also, before using the service you need to ensure that all topics used in your config
exist in kafka. Topics should be prefixed with a datacenter name (default is `default`). Also,
each topic must have a retry topic. So, if you are using a topic named `test_topic`, the follwing
topics must exist in kafka:
```
 - 'default.test_topic'
 - 'default.change-prop.retry.test_topic'
```

## Bug Reporting
The service is maintained by the [Wikimedia Services Team](https://www.mediawiki.org/wiki/Wikimedia_Services).
For bug reporting use [EventBus project on Phabricator](https://phabricator.wikimedia.org/tag/eventbus/)
or [#wikimedia-services](https://kiwiirc.com/client/irc.freenode.net:+6697/#teleirc) IRC channel on freenode.

