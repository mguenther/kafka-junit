package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ObserveKeyValuesTest {

    @Test
    @DisplayName("should preserve constructor arguments")
    public void shouldPreserveConstructorArguments() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10).useDefaults();

        assertThat(observeRequest.getTopic()).isEqualTo("test");
        assertThat(observeRequest.getExpected()).isEqualTo(10);
        assertThat(observeRequest.getObservationTimeMillis()).isEqualTo(ObserveKeyValues.DEFAULT_OBSERVATION_TIME_MILLIS);
    }

    @Test
    @DisplayName("should use defaults if not overridden")
    public void shouldUseDefaultsIfNotOverridden() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10).useDefaults();
        final Properties props = observeRequest.getConsumerProps();

        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
        assertThat(props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_uncommitted");
        assertThat(observeRequest.isIncludeMetadata()).isFalse();
        assertThat(observeRequest.getSeekTo().isEmpty()).isTrue();
    }

    @Test
    @DisplayName("observeFor should override default observation time")
    public void observeForShouldOverrideDefaultObservationTime() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .observeFor(10, TimeUnit.SECONDS)
                .build();

        assertThat(observeRequest.getObservationTimeMillis()).isEqualTo((int) TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    @DisplayName("with should override default setting of the given parameter with the given value")
    public void withShouldOverrideDefaultSetting() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        assertThat(observeRequest.getConsumerProps().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
    }

    @Test
    @DisplayName("withAll should override the default settings of the given parameters with the resp. values")
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000L);

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .withAll(overrides)
                .build();

        assertThat(observeRequest.getConsumerProps().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
        assertThat(observeRequest.getConsumerProps().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(1000L);
    }

    @Test
    @DisplayName("includeMetadata should override its default setting")
    public void includeMetadataShouldOverrideItsDefaultSetting() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .includeMetadata()
                .build();

        assertThat(observeRequest.isIncludeMetadata()).isTrue();
    }

    @Test
    @DisplayName("seekTo should preserve seek settings")
    public void seekToShouldPreserveSeekSettings() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .seekTo(0, 1L)
                .seekTo(Collections.singletonMap(1, 2L))
                .build();

        assertThat(observeRequest.getSeekTo().size()).isEqualTo(2);
        assertThat(observeRequest.getSeekTo().get(0)).isEqualTo(1L);
        assertThat(observeRequest.getSeekTo().get(1)).isEqualTo(2L);
    }
}
