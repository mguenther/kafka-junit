package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendValuesTransactionalTest {

    @Test
    @DisplayName("should preserve constructor arguments")
    public void shouldPreserveConstructorArguments() {

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList("a"))
                .useDefaults();

        assertThat(sendRequest.getValuesPerTopic().containsKey("test-topic")).isTrue();
        assertThat(sendRequest.getValuesPerTopic().get("test-topic").contains("a")).isTrue();
    }

    @Test
    @DisplayName("should be able to close over records for multiple topics")
    public void shouldBeAbleToCloseOverRecordsForMultipleTopics() {

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList("a"))
                .inTransaction("test-topic-2", Collections.singletonList("b"))
                .useDefaults();

        assertThat(sendRequest.getValuesPerTopic().containsKey("test-topic-2")).isTrue();
        assertThat(sendRequest.getValuesPerTopic().get("test-topic-2").contains("b")).isTrue();
    }

    @Test
    @DisplayName("should use defaults if not overridden")
    public void shouldUseDefaultsIfNotOverridden() {

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList("a"))
                .useDefaults();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(sendRequest.shouldFailTransaction()).isFalse();
    }

    @Test
    @DisplayName("should preserve fail transaction setting if overridden")
    public void shouldPreserveFailTransactionSettingIfOverridden() {

        final SendValuesTransactional<String> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList("a"))
                .failTransaction()
                .build();

        assertThat(sendRequest.shouldFailTransaction()).isTrue();
    }

    @Test
    @DisplayName("with should override default setting of given parameter with the given value")
    public void withShouldOverrideDefaultSetting() {

        final SendValuesTransactional<Integer> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(1))
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }

    @Test
    @DisplayName("withAll should override default settings of given parameters with the resp. values")
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final SendValuesTransactional<Integer> sendRequest = SendValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(1))
                .withAll(overrides)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }
}
