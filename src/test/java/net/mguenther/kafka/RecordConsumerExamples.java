package net.mguenther.kafka;

import org.apache.kafka.streams.KeyValue;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.EmbeddedKafkaClusterRule.provisionWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Ignore
public class RecordConsumerExamples {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

    @Before
    public void prepareTestTopic() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "c"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);
    }

    @Test
    public void readValuesConsumesOnlyValuesFromPreviouslySentRecords() throws Exception {

        ReadKeyValues<Object, String> readRequest = ReadKeyValues.<Object, String>from("test-topic").useDefaults();

        List<String> values = cluster.readValues(readRequest);

        assertThat(values.size(), is(3));
    }

    @Test
    public void readConsumesPreviouslySentRecords() throws Exception {

        ReadKeyValues<String, String> readRequest = ReadKeyValues.<String, String>from("test-topic").useDefaults();

        List<KeyValue<String, String>> consumedRecords = cluster.read(readRequest);

        assertThat(consumedRecords.size(), is(3));
    }

    @Test
    public void observeWaitsUntilRequestedNumberOfRecordsHaveBeenConsumed() throws Exception {

        ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.<String, String>on("test-topic", 3).useDefaults();

        cluster.observe(observeRequest); // throws an AssertionError if timeout elapses before requested number of records have been read
    }
}
