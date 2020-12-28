package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendValuesTest {

    @Test
    @DisplayName("should preserve constructor arguments")
    public void shouldPreserveConstructorArguments() {

        final SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b").useDefaults();

        assertThat(sendRequest.getTopic()).isEqualTo("test-topic");
        assertThat(sendRequest.getValues().size()).isEqualTo(2);
        assertThat(sendRequest.getValues().contains("a")).isTrue();
        assertThat(sendRequest.getValues().contains("b")).isTrue();
    }

    @Test
    @DisplayName("should use defaults if not overridden")
    public void shouldUseDefaultsIfNotOverridden() {

        final SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b").useDefaults();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
    }

    @Test
    @DisplayName("with should override the default setting of the given parameter with the given value")
    public void withShouldOverrideDefaultSetting() {

        final SendValues<Integer> sendRequest = SendValues.to("test-topic", 1, 2, 3)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }

    @Test
    @DisplayName("withAll should override the default settings of the given parameters with the resp. values")
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final SendValues<Integer> sendRequest = SendValues.to("test-topic", 1, 2, 3)
                .withAll(overrides)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }
}
