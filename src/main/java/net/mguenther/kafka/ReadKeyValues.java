package net.mguenther.kafka;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Getter
@ToString
@RequiredArgsConstructor
public class ReadKeyValues<K, V> {

    public static final int WITHOUT_LIMIT = -1;
    public static final int DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS = 2_000;

    public static class ReadKeyValuesBuilder<K, V> {

        private final String topic;
        private final Properties consumerProps = new Properties();
        private int limit = WITHOUT_LIMIT;
        private int maxTotalPollTimeMillis = DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS;

        public ReadKeyValuesBuilder(final String topic) {
            this.topic = topic;
        }

        public ReadKeyValuesBuilder<K, V> unlimited() {
            this.limit = WITHOUT_LIMIT;
            return this;
        }

        public ReadKeyValuesBuilder<K, V> withLimit(final int limit) {
            this.limit = limit;
            return this;
        }

        public ReadKeyValuesBuilder<K, V> withMaxTotalPollTime(final int duration, final TimeUnit unit) {
            this.maxTotalPollTimeMillis = (int) unit.toMillis(duration);
            return this;
        }

        public <T> ReadKeyValuesBuilder<K, V> with(final String propertyName, final T value) {
            consumerProps.put(propertyName, value);
            return this;
        }

        public <T> ReadKeyValuesBuilder<K, V> withAll(final Properties consumerProps) {
            this.consumerProps.putAll(consumerProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (consumerProps.get(propertyName) != null) return;
            consumerProps.put(propertyName, value);
        }

        public ReadKeyValues<K, V> useDefaults() {
            consumerProps.clear();
            limit = WITHOUT_LIMIT;
            maxTotalPollTimeMillis = DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS;
            return build();
        }

        public ReadKeyValues<K, V> build() {
            ifNonExisting(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            ifNonExisting(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            ifNonExisting(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            ifNonExisting(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            ifNonExisting(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            ifNonExisting(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
            ifNonExisting(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
            return new ReadKeyValues<>(topic, limit, maxTotalPollTimeMillis, consumerProps);
        }
    }

    private final String topic;
    private final int limit;
    private final int maxTotalPollTimeMillis;
    private final Properties consumerProps;

    public static <K, V> ReadKeyValuesBuilder<K, V> from(final String topic) {
        return new ReadKeyValuesBuilder<>(topic);
    }
}
