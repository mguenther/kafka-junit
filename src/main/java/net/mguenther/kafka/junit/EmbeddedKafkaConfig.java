package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
@Getter
@ToString
@RequiredArgsConstructor
public class EmbeddedKafkaConfig {

    public static final int DEFAULT_NUMBER_OF_BROKERS = 1;
    public static final int USE_RANDOM_ZOOKEEPER_PORT = 0;

    public static class EmbeddedKafkaConfigBuilder {

        private final Properties properties = new Properties();
        private int numberOfBrokers = DEFAULT_NUMBER_OF_BROKERS;

        private EmbeddedKafkaConfigBuilder() {
            properties.put(KafkaConfig$.MODULE$.PortProp(), "0");
        }

        public EmbeddedKafkaConfigBuilder withNumberOfBrokers(final int numberOfBrokers) {
            this.numberOfBrokers = numberOfBrokers;
            return this;
        }

        public <T> EmbeddedKafkaConfigBuilder with(final String propertyName, final T value) {
            properties.put(propertyName, value);
            return this;
        }

        public EmbeddedKafkaConfigBuilder withAll(final Properties overrides) {
            properties.putAll(overrides);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (properties.get(propertyName) != null) return;
            properties.put(propertyName, value);
        }

        public EmbeddedKafkaConfig build() {

            if (numberOfBrokers > 1 && defaultPortHasBeenOverridden()) {
                final int desiredPort = Integer.parseInt(properties.getProperty(KafkaConfig$.MODULE$.PortProp()));
                final String message = "You configured %s broker instances and try to bind them to the dedicated " +
                        "port %s. This will not work. The broker configuration has been adjusted to use ephemeral " +
                        "ports instead.";
                log.warn(String.format(message, numberOfBrokers, desiredPort));
                properties.put(KafkaConfig$.MODULE$.PortProp(), "0");
            }

            ifNonExisting(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), "8000");
            ifNonExisting(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), "10000");
            ifNonExisting(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
            ifNonExisting(KafkaConfig$.MODULE$.NumPartitionsProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.DefaultReplicationFactorProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.MinInSyncReplicasProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.MessageMaxBytesProp(), "1000000");
            ifNonExisting(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
            ifNonExisting(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.SslClientAuthProp(), "none");
            ifNonExisting(KafkaConfig$.MODULE$.AutoLeaderRebalanceEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.LeaderImbalanceCheckIntervalSecondsProp(), 5);
            ifNonExisting(KafkaConfig$.MODULE$.LeaderImbalancePerBrokerPercentageProp(), 1);
            ifNonExisting(KafkaConfig$.MODULE$.UncleanLeaderElectionEnableProp(), "false");
            return new EmbeddedKafkaConfig(numberOfBrokers, properties);
        }

        private boolean defaultPortHasBeenOverridden() {
            return !properties.getProperty(KafkaConfig$.MODULE$.PortProp()).equals("0");
        }
    }

    private final int numberOfBrokers;

    private final Properties brokerProperties;

    public static EmbeddedKafkaConfigBuilder brokers() {
        return new EmbeddedKafkaConfigBuilder();
    }

    /**
     * @return
     *      instance of {@link EmbeddedKafkaConfigBuilder}
     * @deprecated
     *      This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     *      Use {@link #brokers()} instead.
     */
    @Deprecated
    public static EmbeddedKafkaConfigBuilder create() {
        return brokers();
    }

    public static EmbeddedKafkaConfig defaultBrokers() {
        return brokers().build();
    }

    /**
     * @return
     *      instance of {@link EmbeddedKafkaConfig} that contains the default configuration
     *      for all brokers in an embedded Kafka cluster
     * @deprecated
     *      This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     *      Use {@link #defaultBrokers()} instead.
     */
    @Deprecated
    public static EmbeddedKafkaConfig useDefaults() {
        return defaultBrokers();
    }
}
