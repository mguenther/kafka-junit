package net.mguenther.kafka.junit;

import net.mguenther.kafka.junit.connector.InstrumentingConfigBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;

public class ConnectorTest {

    private final String topic = String.format("topic-%s", UUID.randomUUID().toString());
    private final String key = String.format("key-%s", UUID.randomUUID().toString());

    @Rule
    public EmbeddedKafkaCluster cluster = EmbeddedKafkaCluster.provisionWith(EmbeddedKafkaClusterConfig
            .create()
            .provisionWith(
                    EmbeddedConnectConfig
                            .create()
                            .deployConnector(connectorConfig(topic, key))
                            .build())
            .build());

    @Test
    public void connectorShouldBeProvisionedAndEmitRecords() throws Exception {

        cluster.observe(ObserveKeyValues.on(topic, 1)
                .filterOnKeys(k -> k.equalsIgnoreCase(key))
                .build());
    }

    private Properties connectorConfig(final String topic, final String key) {
        return InstrumentingConfigBuilder.create()
                .withTopic(topic)
                .withKey(key)
                .build();
    }
}
