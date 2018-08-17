# Kafka for JUnit

[![Build Status](https://travis-ci.org/mguenther/kafka-junit.svg?branch=master)](https://travis-ci.org/mguenther/kafka-junit.svg) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/net.mguenther.kafka/kafka-junit/badge.svg)](https://maven-badges.herokuapp.com/maven-central/net.mguenther.kafka/kafka-junit)

**Kafka for JUnit** provides JUnit 4.x rule implementations that enables developers to start and stop a complete Kafka cluster comprised of Kafka brokers and distributed Kafka Connect workers from within a JUnit test. It also provides a rich set of convenient accessors to interact with such an embedded Kafka cluster in a lean and non-obtrusive way.

Kafka for JUnit can be used to both whitebox-test individual Kafka-based components of your application or to blackbox-test applications that offer an incoming and/or outgoing Kafka-based interface.

## Using Kafka for JUnit in your tests

Kafka for JUnit provides the necessary infrastructure to exercise your Kafka-based components against an embeddable Kafka cluster. However, Kafka for JUnit got you covered as well if you are simply interested in using the convenient accessors against Kafka clusters that are already present in your infrastructure. Checkout sections "Working with an embedded Kafka cluster" and "Working with an external Kafka cluster" in the [user's guide](https://mguenther.github.io/kafka-junit) for more information.

### Using JUnit 4 rules

```java
public class KafkaTest {

  @Rule
  public EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @Test
  public void shouldWaitForRecordsToBePublished() throws Exception {
    cluster.send(to("test-topic", "a", "b", "c").useDefaults());
    cluster.observe(on("test-topic", 4).useDefaults());
  }
}
```

The same applies for `@ClassRule`.

### What about JUnit 5?

You can use Kafka for JUnit with JUnit 5 of course. However, with its rule-based implementations, Kafka for JUnit is currently tailored for ease of use with JUnit 4. It implements no JUnit Jupiter extension for JUnit 5. There is an issue for that (cf. [ISSUE-004](link:https://github.com/mguenther/kafka-junit/issues/4)), so the development wrt. a JUnit Jupiter extension is planned for a future release. PRs are welcome, though!

### Alternative ways

You do not have to use the JUnit 4 rules if you are not comfortable with them or if you happen to use JUnit 5, which does not support rules any longer. `EmbeddedKafkaCluster` implements the `AutoCloseable` interface, so it is easy to manage it inside your tests yourself.

```java
public class KafkaTest {

  @Test
  public void shouldWaitForRecordsToBePublished() throws Exception {

    try (EmbeddedKafkaCluster cluster = provisionWith(useDefaults())) {
      cluster.send(to("test-topic", "a", "b", "c").useDefaults());
      cluster.observe(on("test-topic", 3).useDefaults());
    }
  }
}
```

### Supported versions of Apache Kafka

| Version of Kafka for JUnit | Supports           |
| -------------------------- | ------------------ |
| 0.1.x                      | Apache Kafka 1.0.0 |
| 0.2.x                      | Apache Kafka 1.0.0 |
| 0.3.x                      | Apache Kafka 1.0.0 |
| 1.0.x                      | Apache Kafka 1.1.1 |
| 2.0.x                      | Apache Kafka 2.0.0 |

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