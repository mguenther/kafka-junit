package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ReadKeyValues.from;
import static net.mguenther.kafka.junit.SendKeyValues.to;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordConsumerTest {

    public EmbeddedKafkaCluster kafka;

    @BeforeEach
    public void prepareEnvironment() throws Exception {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();

        final List<KeyValue<String, String>> records = asList(
                new KeyValue<>("aggregate", "a"),
                new KeyValue<>("aggregate", "b"),
                new KeyValue<>("aggregate", "c"));

        kafka.send(to("test-topic", records));
    }

    @AfterEach
    public void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("readValues should consume only values from previously sent records")
    public void readValuesConsumesOnlyValuesFromPreviouslySentRecords() throws Exception {

        final List<String> values = kafka.readValues(from("test-topic"));

        assertThat(values.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("read should consume only values from previously sent records")
    public void readConsumesPreviouslySentRecords() throws Exception {

        final List<KeyValue<String, String>> consumedRecords = kafka.read(from("test-topic"));

        assertThat(consumedRecords.size()).isEqualTo(3);
    }

    @Test
    @DisplayName("read should consume only values with a custom type that have been sent previously")
    public void readConsumesPreviouslySentCustomValueTypedRecords() throws Exception {

        final List<KeyValue<String, Long>> records = asList(
                new KeyValue<>("min", Long.MIN_VALUE),
                new KeyValue<>("max", Long.MAX_VALUE));

        kafka.send(to("test-topic-value-types", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class));

        final List<KeyValue<String, Long>> consumedRecords = kafka.read(from("test-topic-value-types", Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class));

        assertThat(consumedRecords.size()).isEqualTo(records.size());
    }

    @Test
    @DisplayName("read should only consume records with custom types for key and values that have been sent previously")
    public void readConsumesPreviouslySentCustomKeyValueTypedRecords() throws Exception {

        final List<KeyValue<Integer, Long>> records = asList(
                new KeyValue<>(1, Long.MIN_VALUE),
                new KeyValue<>(2, Long.MAX_VALUE));

        kafka.send(to("test-topic-key-value-types", records)
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class));

        final List<KeyValue<Integer, Long>> consumedRecords = kafka.read(from("test-topic-key-value-types", Integer.class, Long.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class));

        assertThat(consumedRecords.size()).isEqualTo(records.size());
    }

    @Test
    @DisplayName("read should only consume records that pass the key filter")
    public void readConsumesOnlyRecordsThatPassKeyFilter() throws Exception {

        final List<KeyValue<String, Integer>> records = asList(
                new KeyValue<>("1", 1),
                new KeyValue<>("2", 2),
                new KeyValue<>("3", 3),
                new KeyValue<>("4", 4));

        kafka.send(to("test-topic-key-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class));

        final Predicate<String> keyFilter = k -> Integer.parseInt(k) % 2 == 0;

        final List<KeyValue<String, Integer>> consumedRecords = kafka.read(from("test-topic-key-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnKeys(keyFilter));

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().map(KeyValue::getKey).allMatch(keyFilter)).isTrue();
    }

    @Test
    @DisplayName("read should only consume those records that pass the value filter")
    public void readConsumesOnlyRecordsThatPassValueFilter() throws Exception {

        final List<KeyValue<String, Integer>> records = asList(
                new KeyValue<>("1", 1),
                new KeyValue<>("2", 2),
                new KeyValue<>("3", 3),
                new KeyValue<>("4", 4));

        kafka.send(to("test-topic-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class));

        final Predicate<Integer> valueFilter = v -> v % 2 == 1;

        final List<KeyValue<String, Integer>> consumedRecords = kafka.read(from("test-topic-value-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnValues(valueFilter));

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().map(KeyValue::getValue).allMatch(valueFilter)).isTrue();
    }

    @Test
    @DisplayName("read should only consume those records that pass the header filter")
    public void readConsumesOnlyRecordsThatPassHeaderFilter() throws Exception {

        final Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        final Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        final List<KeyValue<String, Integer>> records = asList(
                new KeyValue<>("1", 1, headersA),
                new KeyValue<>("2", 2, headersB));

        kafka.send(to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class));

        final Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("a");

        final List<KeyValue<String, Integer>> consumedRecords = kafka.read(from("test-topic-header-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnHeaders(headerFilter));

        assertThat(consumedRecords.size()).isEqualTo(1);
        assertThat(new String(consumedRecords.get(0).getHeaders().lastHeader("aggregate").value())).isEqualTo("a");
    }

    @Test
    @DisplayName("read should only consume those records that pass both key and value filter")
    public void readConsumesOnlyRecordsThatPassBothKeyAndValueFilter() throws Exception {

        List<KeyValue<String, Integer>> records = Stream.iterate(1, k -> k + 1)
                .limit(30)
                .map(i -> new KeyValue<>(String.format("%s", i), i))
                .collect(Collectors.toList());

        SendKeyValues<String, Integer> sendRequest = to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 3 == 0;
        Predicate<Integer> valueFilter = v -> v % 5 == 0;

        ReadKeyValues<String, Integer> readRequest = from("test-topic-key-value-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        List<KeyValue<String, Integer>> consumedRecords = kafka.read(readRequest);

        Predicate<KeyValue<String, Integer>> combinedFilter = kv -> keyFilter.test(kv.getKey()) && valueFilter.test(kv.getValue());

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().allMatch(combinedFilter)).isTrue();
    }

    @Test
    @DisplayName("readValues should only consume those records that pass the header filter")
    public void readValuesConsumesOnlyRecordsThatPassHeaderFilter() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("a");

        ReadKeyValues<String, Integer> readRequest = from("test-topic-header-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnHeaders(headerFilter)
                .build();

        List<Integer> values = kafka.readValues(readRequest);

        assertThat(values.size()).isEqualTo(1);
        assertThat(values.get(0)).isEqualTo(1);
    }

    @Test
    @DisplayName("observe should wait until the requested number of records have been consumed")
    public void observeWaitsUntilRequestedNumberOfRecordsHaveBeenConsumed() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3).useDefaults();

        int observedRecords = kafka.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(3);
    }

    @Test
    @DisplayName("observe should throw an AssertionError if its timeout elapses")
    public void observeThrowsAnAssertionErrorIfTimeoutElapses() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 4)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        Assertions.assertThrows(AssertionError.class, () -> kafka.observe(observeRequest));
    }

    @Test
    @DisplayName("observe should wait until the requested number of records with a custom type have been consumed")
    public void observeWaitsUntilRequestedNumberOfCustomValueTypedRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<String, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>("min", Long.MIN_VALUE));
        records.add(new KeyValue<>("max", Long.MAX_VALUE));

        SendKeyValues<String, Long> sendRequest = to("test-topic-value-types", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        kafka.send(sendRequest);

        ObserveKeyValues<String, Long> observeRequest = ObserveKeyValues.on("test-topic-value-types", 2, Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        int observedRecords = kafka.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(2);
    }

    @Test
    @DisplayName("observe should wait until the requested number of records with custom key and value have been consumed")
    public void observeWaitsUntilRequestedNumberOfCustomKeyValueTypedRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<Integer, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>(1, Long.MIN_VALUE));
        records.add(new KeyValue<>(2, Long.MAX_VALUE));

        SendKeyValues<Integer, Long> sendRequest = to("test-topic-key-value-types", records)
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        kafka.send(sendRequest);

        ObserveKeyValues<Integer, Long> observeRequest = ObserveKeyValues.on("test-topic-key-value-types", 2, Integer.class, Long.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        int observedRecords = kafka.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(2);
    }

    @Test
    @DisplayName("observe should wait until the requested number of filtered records have been consumed")
    public void observeWaitsUntilRequestedNumberOfFilteredRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<String, Integer>> records = Stream.iterate(1, k -> k + 1)
                .limit(30)
                .map(i -> new KeyValue<>(String.format("%s", i), i))
                .collect(Collectors.toList());

        SendKeyValues<String, Integer> sendRequest = to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 3 == 0;
        Predicate<Integer> valueFilter = v -> v % 5 == 0;

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-key-value-filter", 2, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        List<KeyValue<String, Integer>> observedRecords = kafka.observe(observeRequest);

        Predicate<KeyValue<String, Integer>> combinedFilter = kv -> keyFilter.test(kv.getKey()) && valueFilter.test(kv.getValue());

        assertThat(observedRecords.stream().allMatch(combinedFilter)).isTrue();
    }

    @Test
    @DisplayName("observe should retain header filters")
    public void observeShouldRetainHeaderFilters() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("b");

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-header-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnHeaders(headerFilter)
                .build();

        List<KeyValue<String, Integer>> observedRecords = kafka.observe(observeRequest);

        assertThat(observedRecords.size()).isEqualTo(1);
        assertThat(observedRecords.get(0).getValue()).isEqualTo(2);
    }

    @Test
    @DisplayName("observe should throw an AssertionError if no record passes the filter and its timeout elapses")
    public void observeShouldThrowAnAssertionErrorIfNoRecordPassesTheFilterAndTimeoutElapses() throws Exception {

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1));
        records.add(new KeyValue<>("2", 2));
        records.add(new KeyValue<>("3", 3));
        records.add(new KeyValue<>("4", 4));

        SendKeyValues<String, Integer> sendRequest = to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 2 == 0;
        Predicate<Integer> valueFilter = v -> v % 2 == 1;

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-key-value-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        Assertions.assertThrows(AssertionError.class, () -> {
            kafka.observe(observeRequest);
        });
    }

    @Test
    @DisplayName("observe should throw an AssertionError if no record passes the header filter and its timeout elapses")
    public void observeShouldThrowAnAssertionErrorIfNoRecordPassesTheHeaderFilterAndTimeoutElapses() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        kafka.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("c"); // not existing

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-header-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnHeaders(headerFilter)
                .build();

        Assertions.assertThrows(AssertionError.class, () -> kafka.observe(observeRequest));
    }

    @Test
    @DisplayName("read should include record metadata if explicitly enabled")
    public void readShouldIncludeMetadataIfExplicitlyEnabled() throws Exception {

        ReadKeyValues<String, String> readRequest = from("test-topic")
                .includeMetadata()
                .build();

        List<KeyValue<String, String>> records = kafka.read(readRequest);

        assertThat(records.stream().allMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("All records must include a reference to the topic-partition-offset if includeMetadata is set to true.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getTopic)
                .allMatch(topic -> topic.equalsIgnoreCase("test-topic")))
                .withFailMessage("All records must include a reference to topic 'test-topic'.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getPartition)
                .allMatch(partition -> partition == 0))
                .withFailMessage("All records must include the correct topic-partition.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getOffset)
                .allMatch(offset -> offset >= 0))
                .withFailMessage("All records must include non-negative partition offsets.")
                .isTrue();
    }

    @Test
    @DisplayName("read should not include any record metadata if not enabled")
    public void readShouldNotIncludeMetadataIfNotExplicitlyEnabled() throws Exception {

        ReadKeyValues<String, String> readRequest = from("test-topic").useDefaults();

        List<KeyValue<String, String>> records = kafka.read(readRequest);

        assertThat(records.stream().noneMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("None of the returned record must include a reference to topic-partition-offset.")
                .isTrue();
    }

    @Test
    @DisplayName("observe should include record metadata if explicitly enabled")
    public void observeShouldIncludeMetadataIfExplicitlyEnabled() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3)
                .includeMetadata()
                .build();

        List<KeyValue<String, String>> records = kafka.observe(observeRequest);

        assertThat(records.stream().allMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("All records must include a reference to the topic-partition-offset if includeMetadata is set to true.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getTopic)
                .allMatch(topic -> topic.equalsIgnoreCase("test-topic")))
                .withFailMessage("All records must include a reference to topic 'test-topic'.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getPartition)
                .allMatch(partition -> partition == 0))
                .withFailMessage("All records must include the correct topic-partition.")
                .isTrue();
        assertThat(records
                .stream()
                .map(KeyValue::getMetadata)
                .map(Optional::get)
                .map(KeyValueMetadata::getOffset)
                .allMatch(offset -> offset >= 0))
                .withFailMessage("All records must include non-negative partition offsets.")
                .isTrue();
    }

    @Test
    @DisplayName("observe should not include any record metadata if not enabled")
    public void observeShouldNotIncludeMetadataIfNotExplicitlyEnabled() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3).useDefaults();

        List<KeyValue<String, String>> records = kafka.observe(observeRequest);

        assertThat(records.stream().noneMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("None of the returned record must include a reference to topic-partition-offset.")
                .isTrue();
    }

    @Test
    @DisplayName("a seekTo prior to a call to read should skip all messages before the given offset")
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenReadingKeyValues() throws Exception {

        ReadKeyValues<String, String> readRequest = from("test-topic").seekTo(0, 2).build();

        List<KeyValue<String, String>> records = kafka.read(readRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0).getValue()).isEqualTo("c");
    }

    @Test
    @DisplayName("a seekTo prior to a call to readValues should skip all messages before the given offset")
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenReadingValues() throws Exception {

        ReadKeyValues<String, String> readRequest = from("test-topic").seekTo(0, 2).build();

        List<String> records = kafka.readValues(readRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0)).isEqualTo("c");
    }

    @Test
    @DisplayName("a seekTo prior to a call to observe should skip all messages before the given offset")
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenObservingKeyValues() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 1).seekTo(0, 2).build();

        List<KeyValue<String, String>> records = kafka.observe(observeRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0).getValue()).isEqualTo("c");
    }

    @Test
    @DisplayName("a seekTo prior to a call to observeValues should skip all messages before the given offset")
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenObservingValues() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 1).seekTo(0, 2).build();

        List<String> records = kafka.observeValues(observeRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0)).isEqualTo("c");
    }

    @Test
    @DisplayName("when observing key-value pairs a call to seekTo should not restart observation at the given offset for subsequent reads")
    public void whenObservingKeyValuesSeekToShouldNotRestartObservationAtGivenOffsetForSubsequentReads() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 2)
                .seekTo(0, 2)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        // if the implementation of observe would start over at the given offset, then we would the record with value
        // "c" multiple times until the expected number (in this case 2) is met. if this times out and throws an
        // AssertionError, the implementation works as expected.
        Assertions.assertThrows(AssertionError.class, () -> kafka.observe(observeRequest));
    }

    @Test
    @DisplayName("when observing values a call to seekTo should not restart observation at the given offset for subsequent reads")
    public void whenObservingValuesSeekToShouldNotRestartObservationAtGivenOffsetForSubsequentReads() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 2)
                .seekTo(0, 2)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        // if the implementation of observe would start over at the given offset, then we would the record with value
        // "c" multiple times until the expected number (in this case 2) is met. if this times out and throws an
        // AssertionError, the implementation works as expected.
        Assertions.assertThrows(AssertionError.class, () -> kafka.observeValues(observeRequest));
    }
}
