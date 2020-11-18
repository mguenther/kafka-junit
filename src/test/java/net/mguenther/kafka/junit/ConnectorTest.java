package net.mguenther.kafka.junit;

import net.mguenther.kafka.junit.connector.InstrumentingConfigBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

import static net.mguenther.kafka.junit.EmbeddedConnectConfig.kafkaConnect;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.newClusterConfig;

public class ConnectorTest {

    private final String topic = String.format("topic-%s", UUID.randomUUID().toString());

    private final String key = String.format("key-%s", UUID.randomUUID().toString());

    @Rule
    public EmbeddedKafkaCluster kafka = provisionWith(newClusterConfig()
            .configure(kafkaConnect()
                    .deployConnector(connectorConfig(topic, key))));

    @Test
    public void connectorShouldBeProvisionedAndEmitRecords() throws Exception {

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
