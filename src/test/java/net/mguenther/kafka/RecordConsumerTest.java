package net.mguenther.kafka;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.mguenther.kafka.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.EmbeddedKafkaClusterRule.provisionWith;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordConsumerTest {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

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

        ReadKeyValues<Object, String> readRequest = ReadKeyValues.<Object, String>from("test-topic").useDefaults();

        List<String> values = cluster.readValues(readRequest);

        assertThat(values.size()).isEqualTo(3);
    }

    @Test
    public void readConsumesPreviouslySentRecords() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.<String, String>from("test-topic").useDefaults();

        List<KeyValue<String, String>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size()).isEqualTo(3);
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfRecordsHaveBeenConsumed() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.<String, String>on("test-topic", 3).useDefaults();

        int observedRecords = cluster.observe(observeRequest).size();

        assertThat(observedRecords).isEqualTo(3);
    }

    @Test(expected = AssertionError.class)
    public void observeThrowsAnAssertionErrorIfTimeoutElapses() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.<String, String>on("test-topic", 4)
                .observeFor(5, TimeUnit.SECONDS)
                .build();

        cluster.observe(observeRequest);
    }
}
