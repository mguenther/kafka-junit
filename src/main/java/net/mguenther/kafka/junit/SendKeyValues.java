package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

@Getter
@ToString
@RequiredArgsConstructor
public class SendKeyValues<K, V> {

    public static class SendKeyValuesBuilder<K, V> {

        private final String topic;
        private final Collection<KeyValue<K, V>> records = new ArrayList<>();
        private final Properties producerProps = new Properties();

        SendKeyValuesBuilder(final String topic, final Collection<KeyValue<K, V>> records) {
            this.topic = topic;
            this.records.addAll(records);
        }

        public <T> SendKeyValuesBuilder<K, V> with(final String propertyName, final T value) {
            producerProps.put(propertyName, value);
            return this;
        }

        public SendKeyValuesBuilder<K, V> withAll(final Properties producerProps) {
            this.producerProps.putAll(producerProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (producerProps.get(propertyName) != null) return;
            producerProps.put(propertyName, value);
        }

        public SendKeyValues<K, V> useDefaults() {
            producerProps.clear();
            return build();
        }

        public SendKeyValues<K, V> build() {
            ifNonExisting(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.ACKS_CONFIG, "all");
            return new SendKeyValues<>(topic, records, producerProps);
        }
    }

    private final String topic;
    private final Collection<KeyValue<K, V>> records;
    private final Properties producerProps;

    public static <K, V> SendKeyValuesBuilder<K, V> to(final String topic, final Collection<KeyValue<K, V>> records) {
        return new SendKeyValuesBuilder<>(topic, records);
    }
}
