package net.mguenther.kafka.junit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class TopicConfigTest {

    @Test
    @DisplayName("should preserve constructor arguments")
    void shouldPreserveConstructorArguments() {

        final TopicConfig topicConfig = TopicConfig.withName("test").useDefaults();

        assertThat(topicConfig.getTopic()).isEqualTo("test");
    }

    @Test
    @DisplayName("should use defaults if not overridden")
    void shouldUseDefaultsIfNotOverridden() {

        final TopicConfig topicConfig = TopicConfig.withName("test").useDefaults();

        assertThat(topicConfig.getNumberOfPartitions()).isEqualTo(1);
        assertThat(topicConfig.getNumberOfReplicas()).isEqualTo(1);
        assertThat(topicConfig.getProperties().getProperty("cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig.getProperties().getProperty("delete.retention.ms")).isEqualTo("86400000");
        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("1");
    }

    @Test
    @DisplayName("withNumberOfReplicas should override its default setting")
    void withNumberOfReplicasShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.withName("test")
                .withNumberOfReplicas(99)
                .build();

        assertThat(topicConfig.getNumberOfReplicas()).isEqualTo(99);
    }

    @Test
    @DisplayName("withNumberOfPartitions should override its default setting")
    void withNumberOfPartitionsShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.withName("test")
                .withNumberOfPartitions(99)
                .build();

        assertThat(topicConfig.getNumberOfPartitions()).isEqualTo(99);
    }

    @Test
    @DisplayName("with should override the default setting of the given parameter with the given value")
    void withShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.withName("test")
                .with("min.insync.replicas", "2")
                .build();

        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("2");
    }

    @Test
    @DisplayName("withAll should override the default settings of the given parameters with the resp. values")
    void withAllShouldOverrideDefaulSettings() {

        final Properties overrides = new Properties();
        overrides.put("min.insync.replicas", "2");
        overrides.put("delete.retention.ms", "1000");

        final TopicConfig topicConfig = TopicConfig.withName("test")
                .withAll(overrides)
                .build();

        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("2");
        assertThat(topicConfig.getProperties().getProperty("delete.retention.ms")).isEqualTo("1000");
    }
}
