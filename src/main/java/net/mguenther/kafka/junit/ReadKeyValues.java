package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@Getter
@ToString
@RequiredArgsConstructor
public class ReadKeyValues<K, V> {

    public static final int WITHOUT_LIMIT = -1;
    public static final int DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS = 2_000;

    public static class ReadKeyValuesBuilder<K, V> {

        private final String topic;
        private final Class<K> clazzOfK;
        private final Class<V> clazzOfV;
        private final Properties consumerProps = new Properties();
        private Predicate<K> filterOnKeys = key -> true;
        private Predicate<V> filterOnValues = value -> true;
        private Predicate<Headers> filterOnHeaders = value -> true;
        private int limit = WITHOUT_LIMIT;
        private int maxTotalPollTimeMillis = DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS;
        private boolean includeMetadata = false;
        private Map<Integer, Long> seekTo = new HashMap<>();

        ReadKeyValuesBuilder(final String topic, final Class<K> clazzOfK, final Class<V> clazzOfV) {
            this.topic = topic;
            this.clazzOfK = clazzOfK;
            this.clazzOfV = clazzOfV;
        }

        public ReadKeyValuesBuilder<K, V> filterOnKeys(final Predicate<K> filterOnKeys) {
            this.filterOnKeys = filterOnKeys;
            return this;
        }

        public ReadKeyValuesBuilder<K, V> filterOnValues(final Predicate<V> filterOnValues) {
            this.filterOnValues = filterOnValues;
            return this;
        }

        public ReadKeyValuesBuilder<K, V> filterOnHeaders(final Predicate<Headers> filterOnHeaders) {
            this.filterOnHeaders = filterOnHeaders;
            return this;
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

        public ReadKeyValuesBuilder<K, V> includeMetadata() {
            return withMetadata(true);
        }

        public ReadKeyValuesBuilder<K, V> withMetadata(final boolean modifier) {
            this.includeMetadata = modifier;
            return this;
        }

        public ReadKeyValuesBuilder<K, V> seekTo(final int partition, final long offset) {
            seekTo.put(partition, offset);
            return this;
        }

        public ReadKeyValuesBuilder<K, V> seekTo(final Map<Integer, Long> seekTo) {
            this.seekTo.putAll(seekTo);
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
            return new ReadKeyValues<>(topic, limit, maxTotalPollTimeMillis, includeMetadata, seekTo, consumerProps, filterOnKeys, filterOnValues, filterOnHeaders, clazzOfK, clazzOfV);
        }
    }

    private final String topic;
    private final int limit;
    private final int maxTotalPollTimeMillis;
    private final boolean includeMetadata;
    private final Map<Integer, Long> seekTo;
    private final Properties consumerProps;
    private final Predicate<K> filterOnKeys;
    private final Predicate<V> filterOnValues;
    private final Predicate<Headers> filterOnHeaders;
    private final Class<K> clazzOfK;
    private final Class<V> clazzOfV;

    public static ReadKeyValuesBuilder<String, String> from(final String topic) {
        return from(topic, String.class, String.class);
    }

    public static <V> ReadKeyValuesBuilder<String, V> from(final String topic, final Class<V> clazzOfV) {
        return from(topic, String.class, clazzOfV);
    }

    public static <K, V> ReadKeyValuesBuilder<K, V> from(final String topic, final Class<K> clazzOfK, final Class<V> clazzOfV) {
        return new ReadKeyValuesBuilder<>(topic, clazzOfK, clazzOfV);
    }
}
