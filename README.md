# kafka-junit

[![Build Status](https://travis-ci.org/mguenther/kafka-junit.svg?branch=master)](https://travis-ci.org/mguenther/kafka-junit.svg)

This repository contains JUnit rule implementations that enables developers to start and stop a complete Kafka cluster from within a JUnit test. It furthermore provides convenient accessors to interact with the embedded Kafka cluster in a non-obtrusive way.

## Using kafka-junit in your tests

### Using JUnit 4 rules

```java
public class KafkaTest {

  @Rule
  public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

  @Test
  public void shouldWaitForRecordsToBePublished() throws Exception {
    cluster.send(to("test-topic", "a", "b", "c").useDefaults());
    cluster.observe(on("test-topic", 4).useDefaults());
  }
}
```

The same applies for `@ClassRule`.

### What about JUnit 5?

There is currently no support for JUnit 5. PRs are welcome, though!

### Alternative ways

You do not have to use the JUnit 4 rules if you are not comfortable with it or if you happen to use JUnit 5, which does not support rules any longer. `EmbeddedKafkaClusterRule` instantiates `EmbeddedKafkaCluster` and manages its component lifecycle. However, `EmbeddedKafkaCluster` implements the `AutoCloseable` interface, so it is easy to manage it inside your tests yourself.

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

## Interacting with the Kafka cluster

### Producing records

Class `EmbeddedKafkaClusterRule` as well as `EmbeddedKafkaCluster` expose convenience methods for producing new Kafka records. Have a look at the `RecordProducer` interface.

```java
/**
 * Provides the means to send key-value pairs or un-keyed values to a Kafka topic. The send
 * operations a {@code RecordProducer} provides are synchronous in their nature.
 */
public interface RecordProducer {

    /**
     * Sends (un-keyed) values <emph>synchronously</emph> to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @throws ExecutionException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     */
    <V> List<RecordMetadata> send(SendValues<V> sendRequest) throws ExecutionException, InterruptedException;

    /**
     * Sends key-value pairs <emph>synchronously</emph> to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @throws ExecutionException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValues
     */
    <K, V> List<RecordMetadata> send(SendKeyValues<K, V> sendRequest) throws ExecutionException, InterruptedException;
}
```

Calling `send` using an instance of `SendValues` does just that: It produces un-keyed Kafka records that only feature a value. The key of a record that has been produced this way is simply `null`.  If you wish to associate a key, you can do so by passing an instance of `SendKeyValues` to the `send` method. Both `SendValues` and `SendKeyValues` use the [Builder pattern](https://en.wikipedia.org/wiki/Builder_pattern) so that creating the resp. send parameterization is easy and does not pollute your test code with any kind of boilerplate.

Implementations of the `RecordProducer` inteface use the high-level producer API that comes with Apache Kafka. Hence, the underlying producer is a `KafkaProducer`. This `KafkaProducer` is fully parameterizable via the builders of both `SendValues` and `SendKeyValues`.

All `send` operations are executed synchronously.

With these abstractions in place, sending content to your embedded Kafka cluster is easy. Have a look at the following examples (cf. examples taken from class `RecordProducerTest`). One thing you should notice is that you do not have to specify `bootstrap.servers`. `kafka-junit` adjusts a given client configuration so that you can start off with meaningful defaults that work out-of-the-box. You'll only have to provide configuration overrides if it is absolutely necessary for your test.

#### Sending un-keyed values using defaults

```java
SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c").useDefaults();
cluster.send(sendRequest);
```

#### Sending keyed records using defaults

```java
List<KeyValue<String, String>> records = new ArrayList<>();

records.add(new KeyValue<>("aggregate", "a"));
records.add(new KeyValue<>("aggregate", "c"));
records.add(new KeyValue<>("aggregate", "c"));

SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

cluster.send(sendRequest);
```

#### Changing the default configuration of `KafkaProducer`

```java
SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c")
        .with(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
        .build();

cluster.send(sendRequest);
```

### Consuming records

Class `EmbeddedKafkaClusterRule` as well as `EmbeddedKafkaCluster` expose convenience methods for consuming Kafka records. Have a look at the `RecordConsumer` interface.

```java
/**
 * Provides the means to read key-value pairs or un-keyed values from a Kafka topic as well
 * as the possiblity to watch given topics until a certain amount of records have been consumed
 * from them. All of the operations a {@code RecordConsumer} provides are synchronous in their
 * nature.
 */
public interface RecordConsumer {

    /**
     * Reads values from a Kafka topic.
     *
     * @param readRequest
     *      the configuration of the consumer and the read operation it has to carry out
     * @return
     *      unmodifiable {@link java.util.List} of consumed values
     * @see ReadKeyValues
     */
    <V> List<V> readValues(ReadKeyValues<Object, V> readRequest);

    /**
     * Reads key-value pairs from a Kafka topic.
     *
     * @param readRequest
     *      the configuration of the consumer and the read operation it has to carry out
     * @return
     *      unmodifiable {@link java.util.List} of consumed key-value pairs
     * @see ReadKeyValues
     */
    <K, V> List<KeyValue<K, V>> read(ReadKeyValues<K, V> readRequest);

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the values that have been consumed up until this point of throws an
     * {@code AssertionError} if the number of consumed values does not meet the expected
     * number of records.
     *
     * @param observeRequest
     *      the configuration of the consumer and the observe operation it has to carry out
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of values
     * @see ObserveKeyValues
     */
    <V> List<V> observeValues(ObserveKeyValues<Object, V> observeRequest) throws InterruptedException;

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the key-value-pairs that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed key-value-pairs does not meet the expected
     * number of records.
     *
     * @param observeRequest
     *      the configuration of the consumer and the observe operation it has to carry out
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of key-value pairs
     * @see ObserveKeyValues
     */
    <K, V> List<KeyValue<K, V>> observe(ObserveKeyValues<K, V> observeRequest) throws InterruptedException;
}
```

Implementations of the `RecordConsumer` interface use the high-level consumer API that comes with Apache Kafka. Hence, the underlying consumer is a `KafkaConsumer`. This `KafkaConsumer` is fully parameterizable via both `ReadKeyValues` and `ObserveKeyValues`.

All operations are executed synchronously.

With these abstractions in place, reading content from a Kafka topic is easy. As with a `RecordProducer`, there is no need to specify things like `bootstrap.servers` - `kafka-junit` will provide the necessary configuration. Have a look at the following examples (cf. examples taken from class `RecordConsumerTest`).

All of the examples shown underneath provision `test-topic` with three Kafka records, like so:

```java
List<KeyValue<String, String>> records = new ArrayList<>();

records.add(new KeyValue<>("aggregate", "a"));
records.add(new KeyValue<>("aggregate", "c"));
records.add(new KeyValue<>("aggregate", "c"));

SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

cluster.send(sendRequest);
```

#### Consuming only values

```java
ReadKeyValues<Object, String> readRequest = ReadKeyValues
  .<Object, String>from("test-topic")
  .useDefaults();

List<String> values = cluster.readValues(readRequest);

assertThat(values.size(), is(3));
```

#### Consuming key-values

```java
ReadKeyValues<String, String> readRequest = ReadKeyValues
  .<String, String>from("test-topic")
  .useDefaults();

List<KeyValue<String, String>> consumedRecords = cluster.read(readRequest);

assertThat(consumedRecords.size(), is(3));
```

#### Observing a topic until N records have been consumed

```java
ObserveKeyValues<String, String> observeRequest = ObserveKeyValues
  .<String, String>on("test-topic", 3)
  .useDefaults();

cluster.observe(observeRequest);
```

`observe` will throw an `AssertionError` if the observation timeout elapses before the requested number of Kafka records have been read from the topic. However, `observe` also returns the Kafka records that it has read, thus providing a simple waiting mechanism that blocks until the expected amount of records have been read before the test progresses, while at the same time returning all consumed data.

### Managing topics

Class `EmbeddedKafkaClusterRule` as well as `EmbeddedKafkaCluster` expose convenience methods for managing Kafka topics. Have a look at the `TopicManager` interface.

```java
/**
 * Provides the means to manage Kafka topics. All of the operations a {@code TopicManager} provides
 * are synchronous in their nature.
 */
public interface TopicManager {

    /**
     * Creates the topic as defined by the given {@link TopicConfig} synchronously. This
     * method blocks as long as it takes to complete the underlying topic creation request.
     * Please note that this does not imply that the topic is usable directly after this
     * method returns due to an outstanding partition leader election.
     *
     * @param config
     *      provides the settings for the topic to create
     */
    void createTopic(TopicConfig config);

    /**
     * Marks the given {@code topic} for deletion. Please note that topics are not immediately
     * deleted from a Kafka cluster. This method will fail if the configuration of Kafka brokers
     * prohibits topic deletions and if the topic has already been marked for deletion.
     *
     * @param topic
     *      name of the topic that ought to be marked for deletion
     */
    void deleteTopic(String topic);

    /**
     * Determines whether the given {@code topic} exists.
     *
     * @param topic
     *      name of the topic that ought to be checked
     * @return
     *      {@code true} if the topic exists, {@code false} otherwise
     */
    boolean exists(String topic);
}
```

Implementations of the `TopicCreator` interface currently use the `zkclient` library for topic management.

All operations are executed synchronously.

### Creating a topic

```java
cluster.createTopic(TopicConfig.forTopic("test-topic").useDefaults());
```

### Deleting a topic

```java
cluster.deleteTopic("test-topic");
```

Please note that deleting a topic will only set a deletion marker for that topic. The topic may not be deleted immediately after `deleteTopic` completes.

### Determine whether a topic exists

```java
cluster.exists("test-topic");
```

## License

This work is released under the terms of the Apache 2.0 license.