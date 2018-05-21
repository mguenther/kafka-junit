package net.mguenther.kafka;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;

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

        public SendKeyValuesBuilder(final String topic, final Collection<KeyValue<K, V>> records) {
            this.topic = topic;
            this.records.addAll(records);
        }

        public <T> SendKeyValuesBuilder<K, V> with(final String propertyName, final T value) {
            producerProps.put(propertyName, value);
            return this;
        }

        public <T> SendKeyValuesBuilder<K, V> with(final Properties producerProps) {
            producerProps.putAll(producerProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (producerProps.contains(propertyName)) return;
            producerProps.put(propertyName, value);
        }

        public SendKeyValues<K, V> useDefaults() {
            producerProps.clear();
            return build();
        }

        public SendKeyValues<K, V> build() {
            ifNonExisting("key.serializer", StringSerializer.class);
            ifNonExisting("value.serializer", StringSerializer.class);
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
