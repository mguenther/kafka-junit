package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@RequiredArgsConstructor
public class EmbeddedKafkaClusterConfig {

    public static class EmbeddedKafkaClusterConfigBuilder {

        private EmbeddedZooKeeperConfig zooKeeperConfig = EmbeddedZooKeeperConfig.defaultZooKeeper();

        private EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.defaultBrokers();

        private EmbeddedConnectConfig connectConfig = null;

        /**
         * Uses the given {@link EmbeddedZooKeeperConfig} to configure the ZooKeeper instance that
         * runs within the embedded Kafka cluster.
         *
         * @param zooKeeperConfig
         *      represents the configuration for the embedded ZooKeeper instance
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedZooKeeperConfig zooKeeperConfig) {
            this.zooKeeperConfig = zooKeeperConfig;
            return this;
        }

        /**
         * Uses the given {@link EmbeddedZooKeeperConfig.EmbeddedZooKeeperConfigBuilder} to configure the
         * ZooKeeper instance that runs within the embedded Kafka cluster.
         *
         * @param builder
         *      represents the configuration for the embedded ZooKeeper instance
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedZooKeeperConfig.EmbeddedZooKeeperConfigBuilder builder) {
            return configure(builder.build());
        }

        /**
         * Uses the given {@link EmbeddedKafkaConfig} to configure brokers that run within the
         * embedded Kafka cluster. This configuration is applied to all brokers in a multi-broker
         * environment.
         *
         * @param kafkaConfig
         *      represents the configuration for embedded Kafka brokers
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedKafkaConfig kafkaConfig) {
            this.kafkaConfig = kafkaConfig;
            return this;
        }

        /**
         * Uses the given {@link net.mguenther.kafka.junit.EmbeddedKafkaConfig.EmbeddedKafkaConfigBuilder} to
         * configure brokers that run within the embedded Kafka cluster. This configuration is applied to
         * all brokers in a multi-broker environment.
         *
         * @param builder
         *      represents the configuration for embedded Kafka brokers
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedKafkaConfig.EmbeddedKafkaConfigBuilder builder) {
            return configure(builder.build());
        }

        /**
         * Uses the given {@link EmbeddedConnectConfig} to configure Kafka Connect for the embedded
         * Kafka cluster.
         *
         * @param connectConfig
         *      represents the configuration for Kafka Connect
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedConnectConfig connectConfig) {
            this.connectConfig = connectConfig;
            return this;
        }

        /**
         * Uses the given {@link EmbeddedConnectConfig.EmbeddedConnectConfigBuilder} to configure Kafka Connect
         * for the embedded Kafka cluster.
         *
         * @param builder
         *      represents the configuration for Kafka Connect
         * @return
         *      this builder
         */
        public EmbeddedKafkaClusterConfigBuilder configure(final EmbeddedConnectConfig.EmbeddedConnectConfigBuilder builder) {
            return configure(builder.build());
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

    /**
     * @return instance of {@link EmbeddedKafkaClusterConfigBuilder} used to configure
     * the embedded Kafka cluster
     */
    public static EmbeddedKafkaClusterConfigBuilder newClusterConfig() {
        return new EmbeddedKafkaClusterConfigBuilder();
    }

    /**
     * @return instance of {@link EmbeddedKafkaClusterConfig} that contains the default
     * configuration for the embedded Kafka cluster
     */
    public static EmbeddedKafkaClusterConfig defaultClusterConfig() {
        return newClusterConfig().build();
    }
}
