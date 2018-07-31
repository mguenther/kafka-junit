package net.mguenther.kafka.junit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@Getter
@ToString
@RequiredArgsConstructor
public class SendValues<V> {

    public static class SendValuesBuilder<V> {

        private final String topic;
        private final Collection<V> values = new ArrayList<>();
        private final Properties producerProps = new Properties();

        SendValuesBuilder(final String topic, final Collection<V> values) {
            this.topic = topic;
            this.values.addAll(values);
        }

        public <T> SendValuesBuilder<V> with(final String propertyName, final T value) {
            producerProps.put(propertyName, value);
            return this;
        }

        public <T> SendValuesBuilder<V> withAll(final Properties producerProps) {
            this.producerProps.putAll(producerProps);
            return this;
        }

        private <T> void ifNonExisting(final String propertyName, final T value) {
            if (producerProps.get(propertyName) != null) return;
            producerProps.put(propertyName, value);
        }

        public SendValues<V> useDefaults() {
            producerProps.clear();
            return build();
        }

        public SendValues<V> build() {
            ifNonExisting(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            ifNonExisting(ProducerConfig.ACKS_CONFIG, "all");
            return new SendValues<>(topic, values, producerProps);
        }
    }

    private final String topic;
    private final Collection<V> values;
    private final Properties producerProps;

    public static <V> SendValuesBuilder<V> to(final String topic, final Collection<V> values) {
        return new SendValuesBuilder<>(topic, values);
    }

    @SafeVarargs
    public static <V> SendValuesBuilder<V> to(final String topic, final V... values) {
        return new SendValuesBuilder<>(topic, Arrays.asList(values));
    }
}
