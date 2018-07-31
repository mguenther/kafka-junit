package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Properties;

@Getter
@ToString
@RequiredArgsConstructor
public class TopicConfig {

    public static class TopicConfigBuilder {

        private final String topic;
        private final Properties properties = new Properties();
        private int numberOfPartitions = 1;
        private int numberOfReplicas = 1;

        TopicConfigBuilder(final String topic) {
            this.topic = topic;
        }

        public TopicConfigBuilder withNumberOfPartitions(final int numberOfPartitions) {
            this.numberOfPartitions = numberOfPartitions;
            return this;
        }

        public TopicConfigBuilder withNumberOfReplicas(final int numberOfReplicas) {
            this.numberOfReplicas = numberOfReplicas;
            return this;
        }

        public <T> TopicConfigBuilder with(final String propertyName, final T value) {
            properties.put(propertyName, value);
            return this;
        }

        public TopicConfigBuilder withAll(final Properties properties) {
            this.properties.putAll(properties);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (properties.get(propertyName) != null) return;
            properties.put(propertyName, value);
        }

        public TopicConfig useDefaults() {
            properties.clear();
            numberOfPartitions = 1;
            numberOfReplicas = 1;
            return build();
        }

        public TopicConfig build() {
            ifNonExisting("cleanup.policy", "delete");
            ifNonExisting("delete.retention.ms", "86400000");
            ifNonExisting("min.insync.replicas", "2");
            return new TopicConfig(topic, numberOfPartitions, numberOfReplicas, properties);
        }
    }

    private final String topic;
    private final int numberOfPartitions;
    private final int numberOfReplicas;
    private final Properties properties;

    public static TopicConfigBuilder forTopic(final String topic) {
        return new TopicConfigBuilder(topic);
    }
}
