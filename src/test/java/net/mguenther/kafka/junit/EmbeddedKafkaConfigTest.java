package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedKafkaConfigTest {

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig.useDefaults();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp())).isEqualTo("8000");
        assertThat(props.get(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp())).isEqualTo("10000");
        assertThat(props.get(KafkaConfig$.MODULE$.PortProp())).isEqualTo("0");
        assertThat(props.get(KafkaConfig$.MODULE$.NumPartitionsProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp())).isEqualTo("true");
        assertThat(props.get(KafkaConfig$.MODULE$.MessageMaxBytesProp())).isEqualTo("1000000");
        assertThat(props.get(KafkaConfig$.MODULE$.ControlledShutdownEnableProp())).isEqualTo("true");
        assertThat(props.get(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp())).isEqualTo(0);
        assertThat(props.get(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.HostNameProp())).isEqualTo("localhost");
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .create()
                .with(KafkaConfig$.MODULE$.PortProp(), "9092")
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.PortProp())).isEqualTo("9092");
    }

    @Test
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(KafkaConfig$.MODULE$.PortProp(), "9092");
        overrides.put(KafkaConfig$.MODULE$.NumPartitionsProp(), "2");

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .create()
                .withAll(overrides)
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.PortProp())).isEqualTo("9092");
        assertThat(props.get(KafkaConfig$.MODULE$.NumPartitionsProp())).isEqualTo("2");
    }
}
