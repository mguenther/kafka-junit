package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordConsumerTest {

    @Rule
    public EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

    @Before
    public void prepareTestTopic() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);
    }

    @Test
    public void readValuesConsumesOnlyValuesFromPreviouslySentRecords() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic").useDefaults();

        List<String> values = cluster.readValues(readRequest);

        assertThat(values.size()).isEqualTo(3);
    }

    @Test
    public void readConsumesPreviouslySentRecords() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic").useDefaults();

        List<KeyValue<String, String>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(3);
    }

    @Test
    public void readConsumesPreviouslySentCustomValueTypedRecords()  throws Exception {

        List<KeyValue<String, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>("min", Long.MIN_VALUE));
        records.add(new KeyValue<>("max", Long.MAX_VALUE));

        SendKeyValues<String, Long> sendRequest = SendKeyValues.to("test-topic-value-types", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        cluster.send(sendRequest);

        ReadKeyValues<String, Long> readRequest = ReadKeyValues.from("test-topic-value-types", Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        List<KeyValue<String, Long>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(records.size());
    }

    @Test
    public void readConsumesPreviouslySentCustomKeyValueTypedRecords() throws Exception {

        List<KeyValue<Integer, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>(1, Long.MIN_VALUE));
        records.add(new KeyValue<>(2, Long.MAX_VALUE));

        SendKeyValues<Integer, Long> sendRequest = SendKeyValues.to("test-topic-key-value-types", records)
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        cluster.send(sendRequest);

        ReadKeyValues<Integer, Long> readRequest = ReadKeyValues.from("test-topic-key-value-types", Integer.class, Long.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        List<KeyValue<Integer, Long>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(records.size());
    }

    @Test
    public void readConsumesOnlyRecordsThatPassKeyFilter() throws Exception {

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1));
        records.add(new KeyValue<>("2", 2));
        records.add(new KeyValue<>("3", 3));
        records.add(new KeyValue<>("4", 4));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-key-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 2 == 0;

        ReadKeyValues<String, Integer> readRequest = ReadKeyValues.from("test-topic-key-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnKeys(keyFilter)
                .build();

        List<KeyValue<String, Integer>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().map(KeyValue::getKey).allMatch(keyFilter)).isTrue();
    }

    @Test
    public void readConsumesOnlyRecordsThatPassValueFilter() throws Exception {

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1));
        records.add(new KeyValue<>("2", 2));
        records.add(new KeyValue<>("3", 3));
        records.add(new KeyValue<>("4", 4));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<Integer> valueFilter = v -> v % 2 == 1;

        ReadKeyValues<String, Integer> readRequest = ReadKeyValues.from("test-topic-value-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnValues(valueFilter)
                .build();

        List<KeyValue<String, Integer>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().map(KeyValue::getValue).allMatch(valueFilter)).isTrue();
    }

    @Test
    public void readConsumesOnlyRecordsThatPassHeaderFilter() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("a");

        ReadKeyValues<String, Integer> readRequest = ReadKeyValues.from("test-topic-header-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnHeaders(headerFilter)
                .build();

        List<KeyValue<String, Integer>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(1);
        assertThat(new String(consumedRecords.get(0).getHeaders().lastHeader("aggregate").value())).isEqualTo("a");
    }

    @Test
    public void readConsumesOnlyRecordsThatPassBothKeyAndValueFilter() throws Exception {

        List<KeyValue<String, Integer>> records = Stream.iterate(1, k -> k + 1)
                .limit(30)
                .map(i -> new KeyValue<>(String.format("%s", i), i))
                .collect(Collectors.toList());

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 3 == 0;
        Predicate<Integer> valueFilter = v -> v % 5 == 0;

        ReadKeyValues<String, Integer> readRequest = ReadKeyValues.from("test-topic-key-value-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        List<KeyValue<String, Integer>> consumedRecords = cluster.read(readRequest);

        Predicate<KeyValue<String, Integer>> combinedFilter = kv -> keyFilter.test(kv.getKey()) && valueFilter.test(kv.getValue());

        assertThat(consumedRecords.size()).isEqualTo(2);
        assertThat(consumedRecords.stream().allMatch(combinedFilter)).isTrue();
    }

    @Test
    public void readValuesConsumesOnlyRecordsThatPassHeaderFilter() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("a");

        ReadKeyValues<String, Integer> readRequest = ReadKeyValues.from("test-topic-header-filter", Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .filterOnHeaders(headerFilter)
                .build();

        List<Integer> values = cluster.readValues(readRequest);

        assertThat(values.size()).isEqualTo(1);
        assertThat(values.get(0)).isEqualTo(1);
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfRecordsHaveBeenConsumed() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3).useDefaults();

        int observedRecords = cluster.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(3);
    }

    @Test(expected = AssertionError.class)
    public void observeThrowsAnAssertionErrorIfTimeoutElapses() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 4)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        cluster.observe(observeRequest);
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfCustomValueTypedRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<String, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>("min", Long.MIN_VALUE));
        records.add(new KeyValue<>("max", Long.MAX_VALUE));

        SendKeyValues<String, Long> sendRequest = SendKeyValues.to("test-topic-value-types", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        cluster.send(sendRequest);

        ObserveKeyValues<String, Long> observeRequest = ObserveKeyValues.on("test-topic-value-types", 2, Long.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        int observedRecords = cluster.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(2);
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfCustomKeyValueTypedRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<Integer, Long>> records = new ArrayList<>();

        records.add(new KeyValue<>(1, Long.MIN_VALUE));
        records.add(new KeyValue<>(2, Long.MAX_VALUE));

        SendKeyValues<Integer, Long> sendRequest = SendKeyValues.to("test-topic-key-value-types", records)
                .with(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class)
                .build();

        cluster.send(sendRequest);

        ObserveKeyValues<Integer, Long> observeRequest = ObserveKeyValues.on("test-topic-key-value-types", 2, Integer.class, Long.class)
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class)
                .build();

        int observedRecords = cluster.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(2);
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfFilteredRecordsHaveBeenConsumed() throws Exception {

        List<KeyValue<String, Integer>> records = Stream.iterate(1, k -> k + 1)
                .limit(30)
                .map(i -> new KeyValue<>(String.format("%s", i), i))
                .collect(Collectors.toList());

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 3 == 0;
        Predicate<Integer> valueFilter = v -> v % 5 == 0;

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-key-value-filter", 2, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        List<KeyValue<String, Integer>> observedRecords = cluster.observe(observeRequest);

        Predicate<KeyValue<String, Integer>> combinedFilter = kv -> keyFilter.test(kv.getKey()) && valueFilter.test(kv.getValue());

        assertThat(observedRecords.stream().allMatch(combinedFilter)).isTrue();
    }

    @Test
    public void observeShouldRetainHeaderFilters() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("b");

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-header-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnHeaders(headerFilter)
                .build();

        List<KeyValue<String, Integer>> observedRecords = cluster.observe(observeRequest);

        assertThat(observedRecords.size()).isEqualTo(1);
        assertThat(observedRecords.get(0).getValue()).isEqualTo(2);
    }

    @Test(expected = AssertionError.class)
    public void observeShouldThrowAnAssertionErrorIfNoRecordPassesTheFilterAndTimeoutElapses() throws Exception {

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1));
        records.add(new KeyValue<>("2", 2));
        records.add(new KeyValue<>("3", 3));
        records.add(new KeyValue<>("4", 4));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-key-value-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<String> keyFilter = k -> Integer.parseInt(k) % 2 == 0;
        Predicate<Integer> valueFilter = v -> v % 2 == 1;

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-key-value-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnKeys(keyFilter)
                .filterOnValues(valueFilter)
                .build();

        cluster.observe(observeRequest);
    }

    @Test(expected = AssertionError.class)
    public void observeShouldThrowAnAssertionErrorIfNoRecordPassesTheHeaderFilterAndTimeoutElapses() throws Exception {

        Headers headersA = new RecordHeaders().add("aggregate", "a".getBytes());
        Headers headersB = new RecordHeaders().add("aggregate", "b".getBytes());

        List<KeyValue<String, Integer>> records = new ArrayList<>();

        records.add(new KeyValue<>("1", 1, headersA));
        records.add(new KeyValue<>("2", 2, headersB));

        SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic-header-filter", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();

        cluster.send(sendRequest);

        Predicate<Headers> headerFilter = headers -> new String(headers.lastHeader("aggregate").value()).equals("c"); // not existing

        ObserveKeyValues<String, Integer> observeRequest = ObserveKeyValues.on("test-topic-header-filter", 1, Integer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class)
                .observeFor(5, TimeUnit.SECONDS)
                .filterOnHeaders(headerFilter)
                .build();

        cluster.observe(observeRequest);
    }

    @Test
    public void readShouldIncludeMetadataIfExplicitlyEnabled() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic")
                .includeMetadata()
                .build();

        List<KeyValue<String, String>> records = cluster.read(readRequest);

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
    public void readShouldNotIncludeMetadataIfNotExplicitlyEnabled() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic").useDefaults();

        List<KeyValue<String, String>> records = cluster.read(readRequest);

        assertThat(records.stream().noneMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("None of the returned record must include a reference to topic-partition-offset.")
                .isTrue();
    }

    @Test
    public void observeShouldIncludeMetadataIfExplicitlyEnabled() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3)
                .includeMetadata()
                .build();

        List<KeyValue<String, String>> records = cluster.observe(observeRequest);

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
    public void observeShouldNotIncludeMetadataIfNotExplicitlyEnabled() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 3).useDefaults();

        List<KeyValue<String, String>> records = cluster.observe(observeRequest);

        assertThat(records.stream().noneMatch(kv -> kv.getMetadata().isPresent()))
                .withFailMessage("None of the returned record must include a reference to topic-partition-offset.")
                .isTrue();
    }

    @Test
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenReadingKeyValues() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic").seekTo(0, 2).build();

        List<KeyValue<String, String>> records = cluster.read(readRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0).getValue()).isEqualTo("c");
    }

    @Test
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenReadingValues() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test-topic").seekTo(0, 2).build();

        List<String> records = cluster.readValues(readRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0)).isEqualTo("c");
    }

    @Test
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenObservingKeyValues() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 1).seekTo(0, 2).build();

        List<KeyValue<String, String>> records = cluster.observe(observeRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0).getValue()).isEqualTo("c");
    }

    @Test
    public void seekToShouldSkipAllMessagesBeforeGivenOffsetWhenObservingValues() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 1).seekTo(0, 2).build();

        List<String> records = cluster.observeValues(observeRequest);

        assertThat(records.size()).isEqualTo(1);
        assertThat(records.get(0)).isEqualTo("c");
    }

    @Test(expected = AssertionError.class)
    public void whenObservingKeyValuesSeekToShouldNotRestartObservationAtGivenOffsetForSubsequentReads() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 2)
                .seekTo(0, 2)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        // if the implementation of observe would start over at the given offset, then we would the record with value
        // "c" multiple times until the expected number (in this case 2) is met. if this times out and throws an
        // AssertionError, the implementation works as expected.
        cluster.observe(observeRequest);
    }

    @Test(expected = AssertionError.class)
    public void whenObservingValuesSeekToShouldNotRestartObservationAtGivenOffsetForSubsequentReads() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test-topic", 2)
                .seekTo(0, 2)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        // if the implementation of observe would start over at the given offset, then we would the record with value
        // "c" multiple times until the expected number (in this case 2) is met. if this times out and throws an
        // AssertionError, the implementation works as expected.
        cluster.observeValues(observeRequest);
    }
}
