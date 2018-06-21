package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

@Getter
@ToString
@RequiredArgsConstructor
public class EmbeddedConnectConfig {

    @RequiredArgsConstructor
    public static class EmbeddedConnectConfigBuilder {

        private final String workerId;
        private final Properties properties = new Properties();
        private final List<Properties> connectorProps = new ArrayList<>();

        EmbeddedConnectConfigBuilder() {
            this(UUID.randomUUID().toString().substring(0, 7));
        }

        public <T> EmbeddedConnectConfigBuilder with(final String propertyName, final T value) {
            properties.put(propertyName, value);
            return this;
        }

        public EmbeddedConnectConfigBuilder withAll(final Properties overrides) {
            properties.putAll(overrides);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (properties.get(propertyName) != null) return;
            properties.put(propertyName, value);
        }

        public EmbeddedConnectConfigBuilder deployConnector(final Properties connectorProps) {
            this.connectorProps.add(connectorProps);
            return this;
        }

        public EmbeddedConnectConfigBuilder deployConnectors(final Properties... connectorProps) {
            this.connectorProps.addAll(Arrays.asList(connectorProps));
            return this;
        }

        public EmbeddedConnectConfig build() {

            ifNonExisting(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
            ifNonExisting(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
            ifNonExisting(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
            ifNonExisting(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
            ifNonExisting("internal.key.converter.schemas.enable", "false");
            ifNonExisting("internal.value.converter.schemas.enable", "false");
            ifNonExisting(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
            ifNonExisting(DistributedConfig.CONFIG_TOPIC_CONFIG, "embedded-connect-config");
            ifNonExisting(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
            ifNonExisting(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "embedded-connect-offsets");
            ifNonExisting(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
            ifNonExisting(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "embedded-connect-status");
            ifNonExisting(DistributedConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString().substring(0, 7));

            return new EmbeddedConnectConfig(workerId, properties, connectorProps);
        }
    }

    private final String workerId;
    private final Properties connectProperties;
    private final List<Properties> connectors;

    public static EmbeddedConnectConfigBuilder create() {
        return new EmbeddedConnectConfigBuilder();
    }

    public static EmbeddedConnectConfig useDefaults() {
        return create().build();
    }
}
