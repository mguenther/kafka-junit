package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddedKafkaConfigTest {

    @Test
    @DisplayName("should use defaults if not explicitly overriden")
    void shouldUseDefaultsIfNotOverridden() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultBrokers();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp())).isEqualTo("8000");
        assertThat(props.get(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp())).isEqualTo("10000");
        assertThat(props.get(KafkaConfig$.MODULE$.NumPartitionsProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp())).isEqualTo("true");
        assertThat(props.get(KafkaConfig$.MODULE$.MessageMaxBytesProp())).isEqualTo("1000000");
        assertThat(props.get(KafkaConfig$.MODULE$.ControlledShutdownEnableProp())).isEqualTo("true");
        assertThat(props.get(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp())).isEqualTo(0);
        assertThat(props.get(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp())).isEqualTo("1");
        assertThat(props.get(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp())).isEqualTo("1");
    }

    @Test
    @DisplayName("with(param) should override the corresponding default setting")
    void withShouldOverrideDefaultSetting() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .brokers()
                .with(KafkaConfig$.MODULE$.AdvertisedListenersProp(), "localhost:9092")
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.AdvertisedListenersProp())).isEqualTo("localhost:9092");
    }

    @Test
    @DisplayName("withAll(params) should override the corresponding default settings")
    void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(KafkaConfig$.MODULE$.AdvertisedListenersProp(), "localhost:9092");
        overrides.put(KafkaConfig$.MODULE$.NumPartitionsProp(), "2");

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .brokers()
                .withAll(overrides)
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfig$.MODULE$.AdvertisedListenersProp())).isEqualTo("localhost:9092");
        assertThat(props.get(KafkaConfig$.MODULE$.NumPartitionsProp())).isEqualTo("2");
    }
}
