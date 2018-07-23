package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadKeyValuesTest {

    @Test
    public void shouldPreserveConstructorArguments() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test").useDefaults();

        assertThat(readRequest.getTopic()).isEqualTo("test");
        assertThat(readRequest.getLimit()).isEqualTo(ReadKeyValues.WITHOUT_LIMIT);
        assertThat(readRequest.getMaxTotalPollTimeMillis()).isEqualTo(ReadKeyValues.DEFAULT_MAX_TOTAL_POLL_TIME_MILLIS);
    }

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test").useDefaults();
        final Properties props = readRequest.getConsumerProps();

        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
        assertThat(props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_uncommitted");
        assertThat(readRequest.isIncludeMetadata()).isFalse();
        assertThat(readRequest.getSeekTo().isEmpty()).isTrue();
    }

    @Test
    public void unlimitedShouldNotRestrictLimitSetting() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .withLimit(1)
                .unlimited()
                .build();

        assertThat(readRequest.getLimit()).isEqualTo(ReadKeyValues.WITHOUT_LIMIT);
    }

    @Test
    public void withLimitShouldRestrictLimitSetting() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .withLimit(1)
                .build();

        assertThat(readRequest.getLimit()).isEqualTo(1);
    }

    @Test
    public void withMaxPollTimeShouldOverrideItsDefault() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .withMaxTotalPollTime(10, TimeUnit.SECONDS)
                .build();

        assertThat(readRequest.getMaxTotalPollTimeMillis()).isEqualTo((int) TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void includeMetadataShouldOverrideItsDefaultSetting() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .includeMetadata()
                .build();

        assertThat(readRequest.isIncludeMetadata()).isTrue();
    }

    @Test
    public void seekToShouldPreserveSeekSettings() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .seekTo(0, 1L)
                .seekTo(Collections.singletonMap(1, 2L))
                .build();

        assertThat(readRequest.getSeekTo().size()).isEqualTo(2);
        assertThat(readRequest.getSeekTo().get(0)).isEqualTo(1L);
        assertThat(readRequest.getSeekTo().get(1)).isEqualTo(2L);
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        assertThat(readRequest.getConsumerProps().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
    }

    @Test
    public void withAllShouldOverrideDefaultSettings() {

        final Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        overrides.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000L);

        final ReadKeyValues<String, String> readRequest = ReadKeyValues.from("test")
                .withAll(overrides)
                .build();

        assertThat(readRequest.getConsumerProps().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
        assertThat(readRequest.getConsumerProps().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(1000L);
    }
}
