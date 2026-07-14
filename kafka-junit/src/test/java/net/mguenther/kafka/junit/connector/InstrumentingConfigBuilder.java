package net.mguenther.kafka.junit.connector;

import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.Properties;
import java.util.UUID;

public class InstrumentingConfigBuilder {

    private String topic = String.format("topic-%s", UUID.randomUUID().toString());

    private String key = String.format("key-%s", UUID.randomUUID().toString());

    private final Properties connectorProps = new Properties();

    public InstrumentingConfigBuilder withTopic(final String topic) {
        this.topic = topic;
        return this;
    }

    public InstrumentingConfigBuilder withKey(final String key) {
        this.key = key;
        return this;
    }

    public <T> InstrumentingConfigBuilder with(final String propertyName, final T value) {
        connectorProps.put(propertyName, value);
        return this;
    }

    public <T> InstrumentingConfigBuilder withAll(final Properties connectorProps) {
        this.connectorProps.putAll(connectorProps);
        return this;
    }

    private <T> void ifNonExisting(final String propertyName, final T value) {
        if (connectorProps.get(propertyName) != null) return;
        connectorProps.put(propertyName, value);
    }

    public Properties build() {

        ifNonExisting(ConnectorConfig.NAME_CONFIG, "instrumenting-source-connector");
        ifNonExisting(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "InstrumentingSourceConnector");
        ifNonExisting(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        ifNonExisting("topic", topic);
        ifNonExisting("key", key);

        final Properties copyOfConnectorProps = new Properties();
        copyOfConnectorProps.putAll(connectorProps);

        return copyOfConnectorProps;
    }

    public static InstrumentingConfigBuilder create() {
        return new InstrumentingConfigBuilder();
    }
}
