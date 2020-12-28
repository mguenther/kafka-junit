package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class SendKeyValuesTest {

    @Test
    @DisplayName("should preserve constructor arguments")
    void shouldPreserveConstructorArguments() {

        final Collection<KeyValue<String, String>> records = Collections.singletonList(new KeyValue<>("k", "v"));
        final SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();

        assertThat(sendRequest.getTopic()).isEqualTo("test-topic");
        assertThat(sendRequest.getRecords().size()).isEqualTo(1);
        assertThat(sendRequest.getRecords()).contains(new KeyValue<>("k", "v"));
    }

    @Test
    @DisplayName("should use defaults if not overridden")
    void shouldUseDefaultsIfNotOverridden() {

        final Collection<KeyValue<String, String>> records = Collections.singletonList(new KeyValue<>("k", "v"));
        final SendKeyValues<String, String> sendRequest = SendKeyValues.to("test-topic", records).useDefaults();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
    }

    @Test
    @DisplayName("with should override the default setting of the given parameter with the given value")
    void withShouldOverrideDefaultSetting() {

        final Collection<KeyValue<String, Integer>> records = Collections.singletonList(new KeyValue<>("k", 1));
        final SendKeyValues<String, Integer> sendRequest = SendKeyValues.to("test-topic", records)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }

    @Test
    @DisplayName("withAll should override the default settings of the given parameters with the resp. values")
    void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final Collection<KeyValue<Integer, Integer>> records = Collections.singletonList(new KeyValue<>(1, 1));
        final SendKeyValues<Integer, Integer> sendRequest = SendKeyValues.to("test-topic", records)
                .withAll(overrides)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }
}
