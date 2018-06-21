package net.mguenther.kafka.junit;

import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.junit.ObserveKeyValues.on;

public class ExternalKafkaClusterTest {

    @Rule
    public KafkaContainer kafkaContainer = new KafkaContainer();

    @Test
    public void externalKafkaClusterShouldWorkWithExternalResources() throws Exception {

        ExternalKafkaCluster cluster = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        cluster.send(sendRequest);
        cluster.observe(on("test-topic", 3).useDefaults());
    }
}
