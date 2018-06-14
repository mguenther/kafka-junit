package net.mguenther.kafka.junit;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedConnectConfigTest {

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final EmbeddedConnectConfig config = EmbeddedConnectConfig.useDefaults();
        final Properties props = config.getConnectProperties();

        assertThat(props.get(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG)).isEqualTo("org.apache.kafka.connect.storage.StringConverter");
        assertThat(props.get(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG)).isEqualTo("org.apache.kafka.connect.storage.StringConverter");
        assertThat(props.get(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG)).isEqualTo("org.apache.kafka.connect.json.JsonConverter");
        assertThat(props.get(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG)).isEqualTo("org.apache.kafka.connect.json.JsonConverter");
        assertThat(props.get("internal.key.converter.schemas.enable")).isEqualTo("false");
        assertThat(props.get("internal.value.converter.schemas.enable")).isEqualTo("false");
        assertThat(props.get(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG)).isEqualTo("1");
        assertThat(props.get(DistributedConfig.CONFIG_TOPIC_CONFIG)).isEqualTo("embedded-connect-config");
        assertThat(props.get(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)).isEqualTo("1");
        assertThat(props.get(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)).isEqualTo("embedded-connect-offsets");
        assertThat(props.get(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG)).isEqualTo("1");
        assertThat(props.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG)).isEqualTo("embedded-connect-status");
        assertThat(props.get(DistributedConfig.GROUP_ID_CONFIG)).isNotNull();
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final EmbeddedConnectConfig config = EmbeddedConnectConfig
                .create()
                .with(DistributedConfig.GROUP_ID_CONFIG, "test-group")
                .build();
        final Properties props = config.getConnectProperties();

        assertThat(props.get(DistributedConfig.GROUP_ID_CONFIG)).isEqualTo("test-group");
    }

    @Test
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(DistributedConfig.GROUP_ID_CONFIG, "test-group");
        overrides.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");

        final EmbeddedConnectConfig config = EmbeddedConnectConfig
                .create()
                .withAll(overrides)
                .build();
        final Properties props = config.getConnectProperties();

        assertThat(props.get(DistributedConfig.GROUP_ID_CONFIG)).isEqualTo("test-group");
        assertThat(props.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG)).isEqualTo("status-topic");
    }

    @Test
    public void deployConnectorShouldStoreConnectorConfig() {

        final Properties connectorConfig = new Properties();
        final EmbeddedConnectConfig config = EmbeddedConnectConfig
                .create()
                .deployConnector(connectorConfig)
                .build();

        assertThat(config.getConnectors().size()).isEqualTo(1);
        assertThat(config.getConnectors().contains(connectorConfig));
    }

    @Test
    public void deployConnectorsShouldStoreConnectorConfigs() {

        final EmbeddedConnectConfig config = EmbeddedConnectConfig
                .create()
                .deployConnectors(new Properties(), new Properties())
                .build();

        assertThat(config.getConnectors().size()).isEqualTo(2);
    }
}
