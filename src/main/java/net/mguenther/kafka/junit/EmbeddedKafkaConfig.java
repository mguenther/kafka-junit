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

            ifNonExisting(KafkaConfigConstants.ZOOKEEPER_SESSION_TIMEOUT_MS, "8000");
            ifNonExisting(KafkaConfigConstants.ZOOKEEPER_CONNECTION_TIMEOUT_MS, "10000");
            ifNonExisting(KafkaConfigConstants.NUM_PARTITIONS, "1");
            ifNonExisting(KafkaConfigConstants.DEFAULT_REPLICATION_FACTOR, "1");
            ifNonExisting(KafkaConfigConstants.MIN_INSYNC_REPLICAS, "1");
            ifNonExisting(KafkaConfigConstants.AUTO_CREATE_TOPICS_ENABLE, "true");
            ifNonExisting(KafkaConfigConstants.MESSAGE_MAX_BYTES, "1000000");
            ifNonExisting(KafkaConfigConstants.CONTROLLED_SHUTDOWN_ENABLE, "true");
            ifNonExisting(KafkaConfigConstants.OFFSETS_TOPIC_REPLICATION_FACTOR, "1");
            ifNonExisting(KafkaConfigConstants.GROUP_INITIAL_REBALANCE_DELAY_MS, 0);
            ifNonExisting(KafkaConfigConstants.TRANSACTION_STATE_LOG_REPLICATION_FACTOR, "1");
            ifNonExisting(KafkaConfigConstants.TRANSACTION_STATE_LOG_MIN_ISR, "1");
            ifNonExisting(KafkaConfigConstants.SSL_CLIENT_AUTH, "none");
            ifNonExisting(KafkaConfigConstants.AUTO_LEADER_REBALANCE_ENABLE, "true");
            ifNonExisting(KafkaConfigConstants.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS, 5);
            ifNonExisting(KafkaConfigConstants.LEADER_IMBALANCE_PER_BROKER_PERCENTAGE, 1);
            ifNonExisting(KafkaConfigConstants.UNCLEAN_LEADER_ELECTION_ENABLE, "false");
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
        if (brokerProperties.containsKey(KafkaConfigConstants.LISTENERS)) {
            return brokerProperties.getProperty(KafkaConfigConstants.LISTENERS);
        } else {
            return uniqueListeners.get(brokerIndex);
        }
    }

    /**
     * @return instance of {@link EmbeddedKafkaConfigBuilder}
     */
    public static EmbeddedKafkaConfigBuilder brokers() {
        return new EmbeddedKafkaConfigBuilder();
    }

    /**
     * @return instance of {@link EmbeddedKafkaConfig} that contains the default configuration
     * for all brokers in an embedded Kafka cluster
     */
    public static EmbeddedKafkaConfig defaultBrokers() {
        return brokers().build();
    }
}
