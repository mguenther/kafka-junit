package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValuesTransactional.inTransaction;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordProducerTest {

    @Rule
    public EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

    @Test
    public void sendingUnkeyedRecordsWithDefaults() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c").useDefaults();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(on("test-topic", 3).build()).size())
            .isEqualTo(3);
    }

    @Test
    public void sendingKeyedRecordsWithDefaults() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(on("test-topic", 3).build()).size())
                .isEqualTo(3);
    }

    @Test
    public void sendingUnkeyedRecordsWithAlteredProducerSettings() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c")
                .with(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .build();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(on("test-topic", 3).build()).size())
                .isEqualTo(3);
    }

    @Test
    public void sendingKeyedRecordsWithinTransaction() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        cluster.send(inTransaction("test-topic", records).useDefaults());
        cluster.observeValues(on("test-topic", 3).useDefaults());
    }

    @Test
    public void sendingUnkeyedRecordsWithinTransaction() throws Exception {

        cluster.send(SendValuesTransactional.inTransaction("test-topic", Arrays.asList("a", "b", "c")).useDefaults());
        cluster.observeValues(on("test-topic", 3).useDefaults());
    }

    @Test
    public void sendingUnkeyedRecordsToMultipleTopics() throws Exception {

        SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic-1", Arrays.asList("a", "b"))
                .inTransaction("test-topic-2", Arrays.asList("c", "d"))
                .useDefaults();

        cluster.send(sendRequest);
        cluster.observeValues(on("test-topic-1", 2).useDefaults());
        cluster.observeValues(on("test-topic-2", 2).useDefaults());
    }

    @Test
    public void usingRecordHeaders() throws Exception {

        KeyValue<String, String> record = new KeyValue<>("a", "b");
        record.addHeader("client", "kafka-junit-test".getBytes("utf-8"));

        SendKeyValues<String, String> sendRequest = SendKeyValues
                .to("test-topic", Collections.singletonList(record))
                .useDefaults();

        cluster.send(sendRequest);
        List<KeyValue<String, String>> consumedRecords = cluster.read(ReadKeyValues.from("test-topic").useDefaults());

        assertThat(consumedRecords.size()).isEqualTo(1);
        assertThat(new String(consumedRecords.get(0).getHeaders().lastHeader("client").value())).isEqualTo("kafka-junit-test");
    }

    @Test(expected = AssertionError.class)
    public void valuesOfAbortedTransactionsShouldNotBeVisibleByTransactionalConsumer() throws Exception {

        SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Arrays.asList("a", "b"))
                .failTransaction()
                .build();

        cluster.send(sendRequest);
        cluster.observe(on("test-topic", 2)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .observeFor(5, TimeUnit.SECONDS)
                .build());
    }

    @Test(expected = AssertionError.class)
    public void keyValuesOfAbortedTransactionsShouldNotBeVisibleByTransactionalConsumer() throws Exception {

        SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("a", "b")))
                .failTransaction()
                .build();

        cluster.send(sendRequest);
        cluster.observe(on("test-topic", 1)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .observeFor(5, TimeUnit.SECONDS)
                .build());
    }
}
