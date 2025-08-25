package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddedKafkaConfigTest {

    @Test
    @DisplayName("should use defaults if not explicitly overridden")
    void shouldUseDefaultsIfNotOverridden() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultBrokers();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfigConstants.ZOOKEEPER_SESSION_TIMEOUT_MS)).isEqualTo("8000");
        assertThat(props.get(KafkaConfigConstants.ZOOKEEPER_CONNECTION_TIMEOUT_MS)).isEqualTo("10000");
        assertThat(props.get(KafkaConfigConstants.NUM_PARTITIONS)).isEqualTo("1");
        assertThat(props.get(KafkaConfigConstants.AUTO_CREATE_TOPICS_ENABLE)).isEqualTo("true");
        assertThat(props.get(KafkaConfigConstants.MESSAGE_MAX_BYTES)).isEqualTo("1000000");
        assertThat(props.get(KafkaConfigConstants.CONTROLLED_SHUTDOWN_ENABLE)).isEqualTo("true");
        assertThat(props.get(KafkaConfigConstants.OFFSETS_TOPIC_REPLICATION_FACTOR)).isEqualTo("1");
        assertThat(props.get(KafkaConfigConstants.GROUP_INITIAL_REBALANCE_DELAY_MS)).isEqualTo(0);
        assertThat(props.get(KafkaConfigConstants.TRANSACTION_STATE_LOG_REPLICATION_FACTOR)).isEqualTo("1");
        assertThat(props.get(KafkaConfigConstants.TRANSACTION_STATE_LOG_MIN_ISR)).isEqualTo("1");
    }

    @Test
    @DisplayName("with(param) should override the corresponding default setting")
    void withShouldOverrideDefaultSetting() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .brokers()
                .with(KafkaConfigConstants.ADVERTISED_LISTENERS, "localhost:9092")
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfigConstants.ADVERTISED_LISTENERS)).isEqualTo("localhost:9092");
    }

    @Test
    @DisplayName("withAll(params) should override the corresponding default settings")
    void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(KafkaConfigConstants.ADVERTISED_LISTENERS, "localhost:9092");
        overrides.put(KafkaConfigConstants.NUM_PARTITIONS, "2");

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .brokers()
                .withAll(overrides)
                .build();
        final Properties props = config.getBrokerProperties();

        assertThat(props.get(KafkaConfigConstants.ADVERTISED_LISTENERS)).isEqualTo("localhost:9092");
        assertThat(props.get(KafkaConfigConstants.NUM_PARTITIONS)).isEqualTo("2");
    }

    @Test
    @DisplayName("should yield listeners for multiple brokers")
    void shouldYieldListenersForMultipleBrokers() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig
                .brokers()
                .withNumberOfBrokers(3)
                .build();

        assertThat(config.listenerFor(0)).startsWith("PLAINTEXT://localhost");
        assertThat(config.listenerFor(1)).startsWith("PLAINTEXT://localhost");
        assertThat(config.listenerFor(2)).startsWith("PLAINTEXT://localhost");
    }

    @Test
    @DisplayName("should yield default listener for single broker")
    void shouldYieldDefaultListenerForSingleBroker() {

        final EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultBrokers();

        assertThat(config.listenerFor(0)).isEqualTo("PLAINTEXT://localhost:9092");
    }
}
