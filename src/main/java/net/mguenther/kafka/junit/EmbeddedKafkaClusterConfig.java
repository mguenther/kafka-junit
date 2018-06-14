package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class EmbeddedKafkaClusterConfig {

    public static class EmbeddedKafkaClusterConfigBuilder {

        private EmbeddedZooKeeperConfig zooKeeperConfig = EmbeddedZooKeeperConfig.useDefaults();

        private EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.useDefaults();

        private EmbeddedConnectConfig connectConfig = null;

        public EmbeddedKafkaClusterConfigBuilder provisionWith(final EmbeddedZooKeeperConfig zooKeeperConfig) {
            this.zooKeeperConfig = zooKeeperConfig;
            return this;
        }

        public EmbeddedKafkaClusterConfigBuilder provisionWith(final EmbeddedKafkaConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        public EmbeddedKafkaClusterConfigBuilder provisionWith(final EmbeddedConnectConfig connectConfig) {
            this.connectConfig = connectConfig;
            return this;
        }

        public EmbeddedKafkaClusterConfig build() {
            return new EmbeddedKafkaClusterConfig(zooKeeperConfig, kafkaConfig, connectConfig);
        }
    }

    private final EmbeddedZooKeeperConfig zooKeeperConfig;

    private final EmbeddedKafkaConfig kafkaConfig;

    private final EmbeddedConnectConfig connectConfig;

    public boolean usesConnect() {
        return connectConfig != null;
    }

    public static EmbeddedKafkaClusterConfigBuilder create() {
        return new EmbeddedKafkaClusterConfigBuilder();
    }

    public static EmbeddedKafkaClusterConfig useDefaults() {
        return create().build();
    }
}
