# Kafka for JUnit

[![Build Status](https://travis-ci.org/mguenther/kafka-junit.svg?branch=master)](https://travis-ci.org/mguenther/kafka-junit.svg) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.mguenther.kafka/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.mguenther.kafka/kafka-junit)

Kafka for JUnit enables developers to start and stop a complete Kafka cluster comprised of Kafka brokers and distributed Kafka Connect workers from within a JUnit test. It also provides a rich set of convenient accessors to interact with such an embedded Kafka cluster in a lean and non-obtrusive way.

Kafka for JUnit can be used to both whitebox-test individual Kafka-based components of your application or to blackbox-test applications that offer an incoming and/or outgoing Kafka-based interface.

## Using Kafka for JUnit in your tests

Kafka for JUnit provides the necessary infrastructure to exercise your Kafka-based components against an embeddable Kafka cluster. However, Kafka for JUnit got you covered as well if you are simply interested in using the convenient accessors against Kafka clusters that are already present in your infrastructure. Checkout sections *Working with an embedded Kafka cluster* and *Working with an external Kafka cluster* in the [user's guide](https://mguenther.github.io/kafka-junit) for more information.

```java
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;

class KafkaTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setupKafka() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    void shouldWaitForRecordsToBePublished() throws Exception {
        kafka.send(to("test-topic", "a", "b", "c"));
        kafka.observe(on("test-topic", 4));
    }
}
```

This starts an embedded Kafka cluster and submits three records to the topic named `test-topic`. The call to `kafka.observe(on("test-topic", 3))` watches that same topic for a configurable amount of time and checks if it observes the previously submitted records. If it doesn't, Kafka for JUnit raises an `AssertionError` which would fail the test. Surely, [Kafka for JUnit provides lots of more ways to interact with a Kafka cluster]((https://mguenther.github.io/kafka-junit)).

Since `EmbeddedKafkaCluster` implements the `AutoCloseable` interface, you can achieve the same behavior using a `try-with-resources`-construct.

```java
import org.junit.jupiter.api.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;

class KafkaTest {

  @Test
  void shouldWaitForRecordsToBePublished() throws Exception {

    try (EmbeddedKafkaCluster kafka = provisionWith(defaultClusterConfig())) {
      kafka.start();
      kafka.send(to("test-topic", "a", "b", "c"));
      kafka.observe(on("test-topic", 3));
    }
  }
}
```

### Supported versions of Apache Kafka

| Version of Kafka for JUnit | Supports (up to)   |
| -------------------------- | ------------------ |
| 0.1.x                      | Apache Kafka 1.0.0 |
| 0.2.x                      | Apache Kafka 1.0.0 |
| 0.3.x                      | Apache Kafka 1.0.0 |
| 1.0.x                      | Apache Kafka 1.1.1 |
| 2.0.x                      | Apache Kafka 2.0.0 |
| 2.1.x                      | Apache Kafka 2.1.1 |
| 2.2.x                      | Apache Kafka 2.2.1 |
| 2.3.x                      | Apache Kafka 2.3.0 |
| 2.4.x                      | Apache Kafka 2.4.0 |
| 2.5.x                      | Apache Kafka 2.5.1 |
| 2.6.x                      | Apache Kafka 2.6.0 |
| 2.7.x                      | Apache Kafka 2.7.0 |
| 2.8.0                      | Apache Kafka 2.8.0 |
| 3.0.0                      | Apache Kafka 3.0.0 |
| 3.1.0                      | Apache Kafka 3.1.0 |

## Interacting with the Kafka cluster

See the [comprehensive user's guide](https://mguenther.github.io/kafka-junit) for examples on how to interact with the Kafka cluster from within your JUnit test.

## License

This work is released under the terms of the Apache 2.0 license.

<p>
    <div align="center">
        <div><img src="made-in-darmstadt.jpg"></div>
        <div><a href="https://mguenther.net">mguenther.net</a></div>
    </div>
</p>
