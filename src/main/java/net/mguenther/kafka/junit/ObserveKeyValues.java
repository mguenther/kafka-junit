package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

@Getter
@ToString
@RequiredArgsConstructor
public class ObserveKeyValues<K, V> {

    public static final int DEFAULT_OBSERVATION_TIME_MILLIS = 30_000;

    public static class ObserveKeyValuesBuilder<K, V> {

        private final String topic;
        private final int expected;
        private final Class<K> clazzOfK;
        private final Class<V> clazzOfV;
        private final Properties consumerProps = new Properties();
        private Predicate<K> filterOnKeys = key -> true;
        private Predicate<V> filterOnValues = value -> true;
        private int observationTimeMillis = DEFAULT_OBSERVATION_TIME_MILLIS;

        ObserveKeyValuesBuilder(final String topic, final int expected, final Class<K> clazzOfK, final Class<V> clazzOfV) {
            this.topic = topic;
            this.expected = expected;
            this.clazzOfK = clazzOfK;
            this.clazzOfV = clazzOfV;
        }

        public ObserveKeyValuesBuilder<K, V> observeFor(final int duration, final TimeUnit unit) {
            this.observationTimeMillis = (int) unit.toMillis(duration);
            return this;
        }

        public ObserveKeyValuesBuilder<K, V> filterOnKeys(final Predicate<K> filterOnKeys) {
            this.filterOnKeys = filterOnKeys;
            return this;
        }

        public ObserveKeyValuesBuilder<K, V> filterOnValues(final Predicate<V> filterOnValues) {
            this.filterOnValues = filterOnValues;
            return this;
        }

        public <T> ObserveKeyValuesBuilder<K, V> with(final String propertyName, final T value) {
            consumerProps.put(propertyName, value);
            return this;
        }

        public <T> ObserveKeyValuesBuilder<K, V> withAll(final Properties consumerProps) {
            this.consumerProps.putAll(consumerProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (consumerProps.get(propertyName) != null) return;
            consumerProps.put(propertyName, value);
        }

        public ObserveKeyValues<K, V> useDefaults() {
            consumerProps.clear();
            observationTimeMillis = DEFAULT_OBSERVATION_TIME_MILLIS;
            return build();
        }

        public ObserveKeyValues<K, V> build() {
            ifNonExisting(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            ifNonExisting(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            ifNonExisting(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            ifNonExisting(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            ifNonExisting(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            ifNonExisting(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
            ifNonExisting(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
            return new ObserveKeyValues<>(topic, expected, observationTimeMillis, consumerProps, filterOnKeys, filterOnValues, clazzOfK, clazzOfV);
        }
    }

    private final String topic;
    private final int expected;
    private final int observationTimeMillis;
    private final Properties consumerProps;
    private final Predicate<K> filterOnKeys;
    private final Predicate<V> filterOnValues;
    private final Class<K> clazzOfK;
    private final Class<V> clazzOfV;

    public static ObserveKeyValuesBuilder<String, String> on(final String topic, final int expected) {
        return on(topic, expected, String.class, String.class);
    }

    public static <V> ObserveKeyValuesBuilder<String, V> on(final String topic,
                                                            final int expected,
                                                            final Class<V> clazzOfV) {
        return on(topic, expected, String.class, clazzOfV);
    }

    public static <K, V> ObserveKeyValuesBuilder<K, V> on(final String topic,
                                                          final int expected,
                                                          final Class<K> clazzOfK,
                                                          final Class<V> clazzOfV) {
        return new ObserveKeyValuesBuilder<>(topic, expected, clazzOfK, clazzOfV);
    }
}
