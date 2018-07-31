package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@ToString
@RequiredArgsConstructor
public class SendValuesTransactional<V> {

    public static class SendValuesTransactionalBuilder<V> {

        private final Map<String, Collection<V>> valuesPerTopic = new HashMap<>();
        private final Properties producerPros = new Properties();
        private boolean failTransaction = false;

        SendValuesTransactionalBuilder(final String topic, final Collection<V> values) {
            valuesPerTopic.put(topic, values);
        }

        public SendValuesTransactionalBuilder<V> inTransaction(final String topic, final Collection<V> values) {
            final Collection<V> existingValuesPerTopic = valuesPerTopic.getOrDefault(topic, new ArrayList<>());
            existingValuesPerTopic.addAll(values);
            valuesPerTopic.put(topic, existingValuesPerTopic);
            return this;
        }

        public SendValuesTransactionalBuilder<V> failTransaction() {
            return withFailTransaction(true);
        }

        public SendValuesTransactionalBuilder<V> withFailTransaction(final boolean modifier) {
            failTransaction = modifier;
            return this;
        }

        public <T> SendValuesTransactionalBuilder<V> with(final String propertyName, final T value) {
            producerPros.put(propertyName, value);
            return this;
        }

        public <T> SendValuesTransactionalBuilder<V> withAll(final Properties transactionalProps) {
            this.producerPros.putAll(transactionalProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (producerPros.get(propertyName) != null) return;
            producerPros.put(propertyName, value);
        }

        public SendValuesTransactional<V> useDefaults() {
            producerPros.clear();
            failTransaction = false;
            return build();
        }

        public SendValuesTransactional<V> build() {
            ifNonExisting(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            ifNonExisting(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            ifNonExisting(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            ifNonExisting(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
            ifNonExisting(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
            ifNonExisting(ProducerConfig.ACKS_CONFIG, "all");
            return new SendValuesTransactional<>(valuesPerTopic, failTransaction, producerPros);
        }
    }

    @Getter
    private final Map<String, Collection<V>> valuesPerTopic;

    private final boolean failTransaction;

    @Getter
    private final Properties producerProps;

    public boolean shouldFailTransaction() {
        return failTransaction;
    }

    public static <V> SendValuesTransactionalBuilder<V> inTransaction(final String topic, final Collection<V> values) {
        return new SendValuesTransactionalBuilder<>(topic, values);
    }

    @SafeVarargs
    public static <V> SendValuesTransactionalBuilder<V> inTransaction(final String topic, final V... values) {
        return new SendValuesTransactionalBuilder<>(topic, Arrays.asList(values));
    }
}
