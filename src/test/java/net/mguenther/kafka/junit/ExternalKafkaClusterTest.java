package net.mguenther.kafka.junit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;

import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendKeyValues.to;

@Testcontainers
class ExternalKafkaClusterTest {

    @Container
    private KafkaContainer kafkaContainer = new KafkaContainer();

    @Test
    @DisplayName("should be able to observe records written to an external Kafka cluster")
    void externalKafkaClusterShouldWorkWithExternalResources() throws Exception {

        final ExternalKafkaCluster kafka = ExternalKafkaCluster.at(kafkaContainer.getBootstrapServers());

        final List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("aggregate", "a"));
        records.add(new KeyValue<>("aggregate", "b"));
        records.add(new KeyValue<>("aggregate", "c"));

        kafka.send(to("test-topic", records));
        kafka.observe(on("test-topic", 3).useDefaults());
    }
}
