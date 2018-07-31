package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@ToString
@RequiredArgsConstructor
public class SendKeyValuesTransactional<K, V> {

    public static class SendKeyValuesTransactionalBuilder<K, V> {

        private final Map<String, Collection<KeyValue<K, V>>> recordsPerTopic = new HashMap<>();
        private final Properties producerProps = new Properties();
        private boolean failTransaction = false;

        SendKeyValuesTransactionalBuilder(final String topic, final Collection<KeyValue<K, V>> records) {
            recordsPerTopic.put(topic, records);
        }

        SendKeyValuesTransactionalBuilder(final Map<String, Collection<KeyValue<K, V>>> recordsPerTopic) {
            this.recordsPerTopic.putAll(recordsPerTopic);
        }

        public SendKeyValuesTransactionalBuilder<K, V> inTransaction(final String topic, final Collection<KeyValue<K, V>> records) {
            final Collection<KeyValue<K, V>> existingRecordsForTopic = recordsPerTopic.getOrDefault(topic, new ArrayList<>());
            existingRecordsForTopic.addAll(records);
            recordsPerTopic.put(topic, existingRecordsForTopic);
            return this;
        }

        public SendKeyValuesTransactionalBuilder<K, V> failTransaction() {
            return withFailTransaction(true);
        }

        public SendKeyValuesTransactionalBuilder<K, V> withFailTransaction(final boolean modifier) {
            failTransaction = modifier;
            return this;
        }

        public <T> SendKeyValuesTransactionalBuilder<K, V> with(final String propertyName, final T value) {
            producerProps.put(propertyName, value);
            return this;
        }

        public SendKeyValuesTransactionalBuilder<K, V> withAll(final Properties transactionalProps) {
            this.producerProps.putAll(transactionalProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (producerProps.get(propertyName) != null) return;
            producerProps.put(propertyName, value);
        }

        public SendKeyValuesTransactional<K, V> useDefaults() {
            producerProps.clear();
            failTransaction = false;
            return build();
        }

        public SendKeyValuesTransactional<K, V> build() {
            ifNonExisting(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            ifNonExisting(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            ifNonExisting(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            ifNonExisting(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
            ifNonExisting(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60_000);
            ifNonExisting(ProducerConfig.ACKS_CONFIG, "all");
            return new SendKeyValuesTransactional<>(recordsPerTopic, failTransaction, producerProps);
        }
    }

    @Getter
    private final Map<String, Collection<KeyValue<K, V>>> recordsPerTopic;

    private final boolean failTransaction;

    @Getter
    private final Properties producerProps;

    public boolean shouldFailTransaction() {
        return failTransaction;
    }

    public static <K, V> SendKeyValuesTransactionalBuilder<K, V> inTransaction(final String topic, final Collection<KeyValue<K, V>> records) {
        return new SendKeyValuesTransactionalBuilder<>(topic, records);
    }

    public static <K, V> SendKeyValuesTransactionalBuilder<K, V> inTransaction(final Map<String, Collection<KeyValue<K, V>>> recordsPerTopic) {
        return new SendKeyValuesTransactionalBuilder<>(recordsPerTopic);
    }
}
