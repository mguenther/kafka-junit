package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SendKeyValuesTransactionalTest {

    @Test
    public void shouldPreserveConstructorArguments() {

        final SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("k", "v")))
                .useDefaults();

        assertThat(sendRequest.getRecordsPerTopic().containsKey("test-topic")).isTrue();
        assertThat(sendRequest.getRecordsPerTopic().get("test-topic").contains(new KeyValue<>("k", "v"))).isTrue();
    }

    @Test
    public void shouldBeAbleToCloseOverRecordsForMultipleTopics() {

        final SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("k", "v")))
                .inTransaction("test-topic-2", Collections.singletonList(new KeyValue<>("a", "b")))
                .useDefaults();

        assertThat(sendRequest.getRecordsPerTopic().containsKey("test-topic-2")).isTrue();
        assertThat(sendRequest.getRecordsPerTopic().get("test-topic-2").contains(new KeyValue<>("a", "b"))).isTrue();
    }

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("k", "v")))
                .useDefaults();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(StringSerializer.class);
        assertThat(sendRequest.shouldFailTransaction()).isFalse();
    }

    @Test
    public void shouldPreserveFailTransactionSettingIfOverridden() {

        final SendKeyValuesTransactional<String, String> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("k", "v")))
                .failTransaction()
                .build();

        assertThat(sendRequest.shouldFailTransaction()).isTrue();
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final SendKeyValuesTransactional<String, Integer> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>("a", 1)))
                .with(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }

    @Test
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        overrides.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        final SendKeyValuesTransactional<Long, Integer> sendRequest = SendKeyValuesTransactional
                .inTransaction("test-topic", Collections.singletonList(new KeyValue<>(1L, 2)))
                .withAll(overrides)
                .build();
        final Properties props = sendRequest.getProducerProps();

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)).isEqualTo(LongSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(IntegerSerializer.class);
    }
}
