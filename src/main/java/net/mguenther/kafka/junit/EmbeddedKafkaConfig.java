package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@ToString
@RequiredArgsConstructor
public class EmbeddedKafkaConfig {

    public static final int DEFAULT_NUMBER_OF_BROKERS = 1;

    public static final String DEFAULT_LISTENER = "PLAINTEXT://localhost:9092";

    private static final String LISTENER_TEMPLATE = "PLAINTEXT://localhost:%s";

    public static class EmbeddedKafkaConfigBuilder {

        private final Properties properties = new Properties();
        private int numberOfBrokers = DEFAULT_NUMBER_OF_BROKERS;

        private EmbeddedKafkaConfigBuilder() {
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

            final List<String> listeners = new ArrayList<>(numberOfBrokers);

            if (numberOfBrokers > 1) {
                listeners.addAll(getUniqueEphemeralPorts(numberOfBrokers)
                        .stream()
                        .map(port -> String.format(LISTENER_TEMPLATE, port))
                        .collect(Collectors.toList()));
            } else {
                listeners.add(DEFAULT_LISTENER);
            }

            ifNonExisting(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), "8000");
            ifNonExisting(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), "10000");
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
            return new EmbeddedKafkaConfig(numberOfBrokers, listeners, properties);
        }

        private List<Integer> getUniqueEphemeralPorts(final int howMany) {
            final List<Integer> ephemeralPorts = new ArrayList<>(howMany);
            while (ephemeralPorts.size() < howMany) {
                final int port = generateRandomEphemeralPort();
                if (!ephemeralPorts.contains(port)) {
                    ephemeralPorts.add(port);
                }
            }
            return ephemeralPorts;
        }

        private int generateRandomEphemeralPort() {
            return Math.min((int) (Math.random() * 65535) + 1024, 65535);
        }
    }

    @Getter
    private final int numberOfBrokers;

    private final List<String> uniqueListeners;

    @Getter
    private final Properties brokerProperties;

    public String listenerFor(final int brokerIndex) {
        if (brokerProperties.containsKey(KafkaConfig$.MODULE$.ListenersProp())) {
            return brokerProperties.getProperty(KafkaConfig$.MODULE$.ListenersProp());
        } else {
            return uniqueListeners.get(brokerIndex);
        }
    }

    public static EmbeddedKafkaConfigBuilder brokers() {
        return new EmbeddedKafkaConfigBuilder();
    }

    /**
     * @return instance of {@link EmbeddedKafkaConfigBuilder}
     * @deprecated This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     * Use {@link #brokers()} instead.
     */
    @Deprecated
    public static EmbeddedKafkaConfigBuilder create() {
        return brokers();
    }

    public static EmbeddedKafkaConfig defaultBrokers() {
        return brokers().build();
    }

    /**
     * @return instance of {@link EmbeddedKafkaConfig} that contains the default configuration
     * for all brokers in an embedded Kafka cluster
     * @deprecated This method is deprecated since 2.7.0. Expect it to be removed in a future release.
     * Use {@link #defaultBrokers()} instead.
     */
    @Deprecated
    public static EmbeddedKafkaConfig useDefaults() {
        return defaultBrokers();
    }
}
