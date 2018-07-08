package net.mguenther.kafka.junit;

import kafka.server.KafkaConfig$;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Properties;

@Getter
@ToString
@RequiredArgsConstructor
public class EmbeddedKafkaConfig {

    public static final int USE_RANDOM_ZOOKEEPER_PORT = 0;

    public static class EmbeddedKafkaConfigBuilder {

        private Properties properties = new Properties();

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
            ifNonExisting(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), "8000");
            ifNonExisting(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), "10000");
            ifNonExisting(KafkaConfig$.MODULE$.HostNameProp(), "localhost");
            ifNonExisting(KafkaConfig$.MODULE$.PortProp(), "0");
            ifNonExisting(KafkaConfig$.MODULE$.NumPartitionsProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.MessageMaxBytesProp(), "1000000");
            ifNonExisting(KafkaConfig$.MODULE$.ControlledShutdownEnableProp(), "true");
            ifNonExisting(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.GroupInitialRebalanceDelayMsProp(), 0);
            ifNonExisting(KafkaConfig$.MODULE$.TransactionsTopicReplicationFactorProp(), "1");
            ifNonExisting(KafkaConfig$.MODULE$.TransactionsTopicMinISRProp(), "1");
            return new EmbeddedKafkaConfig(properties);
        }
    }

    private final Properties brokerProperties;

    public static EmbeddedKafkaConfigBuilder create() {
        return new EmbeddedKafkaConfigBuilder();
    }

    public static EmbeddedKafkaConfig useDefaults() {
        return create().build();
    }
}
