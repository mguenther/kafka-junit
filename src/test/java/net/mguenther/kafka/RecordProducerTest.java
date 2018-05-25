package net.mguenther.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.EmbeddedKafkaClusterRule.provisionWith;
import static org.assertj.core.api.Assertions.assertThat;

public class RecordProducerTest {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

    @Test
    public void sendingUnkeyedRecordsWithDefaults() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c").useDefaults();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(ObserveKeyValues.<Object, String>on("test-topic", 3).build()).size())
            .isEqualTo(3);
    }

    @Test
    public void sendingKeyedRecordsWithDefaults() throws Exception {

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "c"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(ObserveKeyValues.<Object, String>on("test-topic", 3).build()).size())
                .isEqualTo(3);
    }

    @Test
    public void sendingUnkeyedRecordsWithAlteredProducerSettings() throws Exception {

        SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b", "c")
                .with(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .with(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
                .build();

        cluster.send(sendRequest);

        assertThat(cluster.observeValues(ObserveKeyValues.<Object, String>on("test-topic", 3).build()).size())
                .isEqualTo(3);
    }
}
