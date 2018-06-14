package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendValuesTest {

    @Test
    public void shouldPreserveConstructorArguments() {

        final SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b").useDefaults();

        assertThat(sendRequest.getTopic()).isEqualTo("test-topic");
        assertThat(sendRequest.getValues().size()).isEqualTo(2);
        assertThat(sendRequest.getValues().contains("a")).isTrue();
        assertThat(sendRequest.getValues().contains("b")).isTrue();
    }

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final SendValues<String> sendRequest = SendValues.to("test-topic", "a", "b").useDefaults();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final SendValues<Integer> sendRequest = SendValues.to("test-topic", 1, 2, 3)
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }

    @Test
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
