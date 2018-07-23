package net.mguenther.kafka.junit.connector;

import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.util.Properties;
import java.util.UUID;

public class InstrumentingConfigBuilder {

    private String topic = String.format("topic-%s", UUID.randomUUID().toString());

    private String key = String.format("key-%s", UUID.randomUUID().toString());

    public InstrumentingConfigBuilder withTopic(final String topic) {
        this.topic = topic;
        return this;
    }

    public InstrumentingConfigBuilder withKey(final String key) {
        this.key = key;
        return this;
    }

    public Properties build() {
        final Properties props = new Properties();
        props.put(ConnectorConfig.NAME_CONFIG, "instrumenting-source-connector");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, "InstrumentingSourceConnector");
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.put("topic", topic);
        props.put("key", key);
        return props;
    }

    public static InstrumentingConfigBuilder create() {
        return new InstrumentingConfigBuilder();
    }
}
