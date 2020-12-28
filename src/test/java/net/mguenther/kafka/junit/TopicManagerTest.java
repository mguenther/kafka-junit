package net.mguenther.kafka.junit;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.TopicConfig.withName;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TopicManagerTest {

    public EmbeddedKafkaCluster kafka;

    @BeforeEach
    public void prepareEnvironment() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    public void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    public void manageTopics() {

        kafka.createTopic(withName("test-topic"));

        assertThat(kafka.exists("test-topic")).isTrue();

        // the topic will not be deleted immediately, but "marked for deletion"
        // hence a check on exists would return "false" directly after deleting
        // the topic
        kafka.deleteTopic("test-topic");
    }

    @Test
    public void fetchLeaderAndIsrShouldRetrieveTheIsr() throws Exception {

        kafka.createTopic(withName("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(1));

        // it takes a couple of seconds until topic-partition assignments are there
        delay(5);

        Map<Integer, LeaderAndIsr> isr = kafka.fetchLeaderAndIsr("test-topic");

        assertThat(isr.size()).isEqualTo(5);
        assertThat(isr.values().stream().allMatch(lai -> lai.getLeader() == 1)).isTrue();
    }

    @Test
    public void fetchTopicConfigShouldRetrieveTheProperConfig() throws Exception {

        kafka.createTopic(withName("test-topic")
                .with("min.insync.replicas", "1"));

        delay(3);

        Properties topicConfig = kafka.fetchTopicConfig("test-topic");

        assertThat(topicConfig.getProperty("min.insync.replicas")).isEqualTo("1");
    }

    @Test
    public void fetchTopicConfigShouldThrowRuntimeExceptionIfTopicDoesNotExist() {
        Assertions.assertThrows(RuntimeException.class, () -> kafka.fetchTopicConfig(UUID.randomUUID().toString()));
    }
}
