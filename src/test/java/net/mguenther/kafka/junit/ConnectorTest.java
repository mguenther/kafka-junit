package net.mguenther.kafka.junit;

import net.mguenther.kafka.junit.connector.InstrumentingConfigBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.UUID;

import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;

class ConnectorTest {

    private final String topic = String.format("topic-%s", UUID.randomUUID().toString());

    private final String key = String.format("key-%s", UUID.randomUUID().toString());

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareEnvironment() {
        kafka = provisionWith(newClusterConfig()
                .configure(kafkaConnect()
                        .deployConnector(connectorConfig(topic, key))));
        kafka.start();
    }

    @AfterEach
    public void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("A given Kafka Connect connector should be provisioned and able to emit records")
    void connectorShouldBeProvisionedAndEmitRecords() throws Exception {

        kafka.observe(ObserveKeyValues.on(topic, 1)
                .filterOnKeys(k -> k.equalsIgnoreCase(key))
                .build());
    }

    private Properties connectorConfig(final String topic, final String key) {
        return InstrumentingConfigBuilder.create()
                .withTopic(topic)
                .withKey(key)
                .with("consumer.override.auto.offset.reset", "latest")
                .build();
    }
}
