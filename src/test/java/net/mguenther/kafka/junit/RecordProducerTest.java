package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValuesTransactional.inTransaction;
import static net.mguenther.kafka.junit.SendValues.to;
import static org.assertj.core.api.Assertions.assertThat;

class RecordProducerTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareEnvironment() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("calling send with non-keyed records and default settings should write all given records to the target topic")
    void sendingUnkeyedRecordsWithDefaults() throws Exception {

        kafka.send(to("test-topic", "a", "b", "c"));

        assertThat(kafka.observeValues(on("test-topic", 3)).size())
                .isEqualTo(3);
    }

    @Test
    @DisplayName("calling send with keyed records and default settings should write all given records to the target topic")
    void sendingKeyedRecordsWithDefaults() throws Exception {

        final List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        kafka.send(SendKeyValues.to("test-topic", records));

        assertThat(kafka.observeValues(on("test-topic", 3)).size())
                .isEqualTo(3);
    }

    @Test
    @DisplayName("calling send with non-keyed records and altered producer settings should write all given records to the target topic")
    void sendingUnkeyedRecordsWithAlteredProducerSettings() throws Exception {

        final SendValues<String> sendRequest = to("test-topic", "a", "b", "c")
                .with(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .build();

        kafka.send(sendRequest);

        assertThat(kafka.observeValues(on("test-topic", 3)).size())
                .isEqualTo(3);
    }

    @Test
    @DisplayName("calling send with keyed records within a transaction should write all given records to the target topic")
    void sendingKeyedRecordsWithinTransaction() throws Exception {

        final List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        kafka.send(inTransaction("test-topic", records));
        kafka.observeValues(on("test-topic", 3));
    }

    @Test
    @DisplayName("calling send with non-keyed records within a transaction should write all given records to the target topic")
    void sendingUnkeyedRecordsWithinTransaction() throws Exception {

        kafka.send(SendValuesTransactional.inTransaction("test-topic", asList("a", "b", "c")));
        kafka.observeValues(on("test-topic", 3));
    }

    @Test
    @DisplayName("calling send with non-keyed records for multiple topics should write all given records to the correct topic")
    void sendingUnkeyedRecordsToMultipleTopics() throws Exception {

        kafka.send(SendValuesTransactional
                .inTransaction("test-topic-1", asList("a", "b"))
                .inTransaction("test-topic-2", asList("c", "d")));
        kafka.observeValues(on("test-topic-1", 2).useDefaults());
        kafka.observeValues(on("test-topic-2", 2).useDefaults());
    }

    @Test
    @DisplayName("record headers should be retained")
    void usingRecordHeaders() throws Exception {

        final KeyValue<String, String> record = new KeyValue<>("a", "b");
        record.addHeader("client", "kafka-junit-test".getBytes(StandardCharsets.UTF_8));

        kafka.send(SendKeyValues.to("test-topic", singletonList(record)));

        final List<KeyValue<String, String>> consumedRecords = kafka.read(ReadKeyValues.from("test-topic"));

        assertThat(consumedRecords.size()).isEqualTo(1);
        assertThat(new String(consumedRecords.get(0).getHeaders().lastHeader("client").value())).isEqualTo("kafka-junit-test");
    }

    @Test
    @DisplayName("non-keyed records written during a failed transaction should not be visible by a transactional consumer")
    void valuesOfAbortedTransactionsShouldNotBeVisibleByTransactionalConsumer() throws Exception {

        kafka.send(SendValuesTransactional
                .inTransaction("test-topic", asList("a", "b"))
                .failTransaction());

        Assertions.assertThrows(AssertionError.class, () -> kafka.observe(on("test-topic", 2)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .observeFor(5, TimeUnit.SECONDS)
                .build()));
    }

    @Test
    @DisplayName("keyed records written during a failed transaction should be not visible by a transactional consumer")
    void keyValuesOfAbortedTransactionsShouldNotBeVisibleByTransactionalConsumer() throws Exception {

        kafka.send(inTransaction("test-topic", singletonList(new KeyValue<>("a", "b"))).failTransaction());

        Assertions.assertThrows(AssertionError.class, () -> kafka.observe(on("test-topic", 1)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .observeFor(5, TimeUnit.SECONDS)));
    }
}
