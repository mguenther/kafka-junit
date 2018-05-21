package net.mguenther.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KeyValue;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.EmbeddedKafkaClusterRule.provisionWith;

@Ignore
public class RecordProducerExamples {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

    @Test
    public void sendingUnkeyedRecordsWithDefaults() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c").useDefaults();

        cluster.send(sendRequest);
    }

    @Test
    public void sendingKeyedRecordsWithDefaults() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "c"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);
    }

    @Test
    public void sendingUnkeyedRecordsWithAlteredProducerSettings() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c")
                .with(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .build();

        cluster.send(sendRequest);
    }
}
