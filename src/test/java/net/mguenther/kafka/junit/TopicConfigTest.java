package net.mguenther.kafka.junit;

import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicConfigTest {

    @Test
    public void shouldPreserveConstructorArguments() {

        final TopicConfig topicConfig = TopicConfig.forTopic("test").useDefaults();

        assertThat(topicConfig.getTopic()).isEqualTo("test");
    }

    @Test
    public void shouldUseDefaultsIfNotOverridden() {

        final TopicConfig topicConfig = TopicConfig.forTopic("test").useDefaults();

        assertThat(topicConfig.getNumberOfPartitions()).isEqualTo(1);
        assertThat(topicConfig.getNumberOfReplicas()).isEqualTo(1);
        assertThat(topicConfig.getProperties().getProperty("cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig.getProperties().getProperty("delete.retention.ms")).isEqualTo("86400000");
        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("2");
    }

    @Test
    public void withNumberOfReplicasShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.forTopic("test")
                .withNumberOfReplicas(99)
                .build();

        assertThat(topicConfig.getNumberOfReplicas()).isEqualTo(99);
    }

    @Test
    public void withNumberOfPartitionsShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.forTopic("test")
                .withNumberOfPartitions(99)
                .build();

        assertThat(topicConfig.getNumberOfPartitions()).isEqualTo(99);
    }

    @Test
    public void withShouldOverrideDefaultSetting() {

        final TopicConfig topicConfig = TopicConfig.forTopic("test")
                .with("min.insync.replicas", "2")
                .build();

        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("2");
    }

    @Test
    public void withAllShouldOverrideDefaulSettings() {

        final Properties overrides = new Properties();
        overrides.put("min.insync.replicas", "2");
        overrides.put("delete.retention.ms", "1000");

        final TopicConfig topicConfig = TopicConfig.forTopic("test")
                .withAll(overrides)
                .build();

        assertThat(topicConfig.getProperties().getProperty("min.insync.replicas")).isEqualTo("2");
        assertThat(topicConfig.getProperties().getProperty("delete.retention.ms")).isEqualTo("1000");
    }
}
