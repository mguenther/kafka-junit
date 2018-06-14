package net.mguenther.kafka.junit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ObserveKeyValuesTest {

    @Test
    public void shouldPreserveConstructorArguments() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10).useDefaults();

        assertThat(observeRequest.getTopic()).isEqualTo("test");
        assertThat(observeRequest.getExpected()).isEqualTo(10);
        assertThat(observeRequest.getObservationTimeMillis()).isEqualTo(ObserveKeyValues.DEFAULT_OBSERVATION_TIME_MILLIS);
    }

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10).useDefaults();
        final Properties props = observeRequest.getConsumerProps();

        assertThat(props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class);
        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
        assertThat(props.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_uncommitted");
    }

    @Test
    public void observeForShouldOverrideDefaultObservationTime() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .observeFor(10, TimeUnit.SECONDS)
                .build();

        assertThat(observeRequest.getObservationTimeMillis()).isEqualTo((int) TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final ObserveKeyValues<String, String> observeRequest = ObserveKeyValues.on("test", 10)
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .build();

        assertThat(observeRequest.getConsumerProps().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
    }

    @Test
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
}
