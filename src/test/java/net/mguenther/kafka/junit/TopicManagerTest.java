package net.mguenther.kafka.junit;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
class TopicManagerTest {

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void prepareEnvironment() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    void tearDownEnvironment() {
        if (kafka != null) kafka.stop();
    }

    @Test
    @DisplayName("should be able to create topics and mark them for deletion")
    void shouldBeAbleToCreateTopicsAndMarkThemForDeletion() {

        kafka.createTopic(withName("test-topic"));

        assertThat(kafka.exists("test-topic")).isTrue();

        // the topic will not be deleted immediately, but "marked for deletion"
        // hence a check on exists would return "false" directly after deleting
        // the topic
        kafka.deleteTopic("test-topic");
    }

    @Test
    @DisplayName("fetchLeaderAndIsr should retrieve the in-sync replica set")
    void fetchLeaderAndIsrShouldRetrieveTheIsr() throws Exception {

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
    @DisplayName("fetchToppiConfig should retrieve the proper config")
    void fetchTopicConfigShouldRetrieveTheProperConfig() throws Exception {

        kafka.createTopic(withName("test-topic")
                .with("min.insync.replicas", "1"));

        delay(3);

        Properties topicConfig = kafka.fetchTopicConfig("test-topic");

        assertThat(topicConfig.getProperty("min.insync.replicas")).isEqualTo("1");
    }

    @Test
    @DisplayName("fetchTopicConfig should throw a RuntimeException if the topic does not exist")
    void fetchTopicConfigShouldThrowRuntimeExceptionIfTopicDoesNotExist() {
        Assertions.assertThrows(RuntimeException.class, () -> kafka.fetchTopicConfig(UUID.randomUUID().toString()));
    }
}
