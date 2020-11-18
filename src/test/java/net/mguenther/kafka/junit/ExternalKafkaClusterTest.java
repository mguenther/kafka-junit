package net.mguenther.kafka.junit;

import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValues.to;

public class ExternalKafkaClusterTest {

    @Rule
    public KafkaContainer kafkaContainer = new KafkaContainer();

    @Test
    public void externalKafkaClusterShouldWorkWithExternalResources() throws Exception {

        final ExternalKafkaCluster kafka = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());

        final List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        kafka.send(to("test-topic", records));
        kafka.observe(on("test-topic", 3).useDefaults());
    }
}
