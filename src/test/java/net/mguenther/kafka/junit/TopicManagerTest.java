package net.mguenther.kafka.junit;

import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.junit.Wait.delay;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class TopicManagerTest {

    @Rule
    public EmbeddedKafkaCluster kafka = provisionWith(useDefaults());

    @Test
    public void manageTopics() {

        kafka.createTopic(TopicConfig.forTopic("test-topic").useDefaults());

        assertThat(kafka.exists("test-topic")).isTrue();

        // the topic will not be deleted immediately, but "marked for deletion"
        // hence a check on exists would return "false" directly after deleting
        // the topic
        kafka.deleteTopic("test-topic");
    }

    @Test
    public void fetchLeaderAndIsrShouldRetrieveTheIsr() throws Exception {

        kafka.createTopic(TopicConfig.forTopic("test-topic")
            .withNumberOfPartitions(5)
            .withNumberOfReplicas(1)
            .build());

        // it takes a couple of seconds until topic-partition assignments are there
        delay(5);

        Map<Integer, LeaderAndIsr> isr = kafka.fetchLeaderAndIsr("test-topic");

        assertThat(isr.size()).isEqualTo(5);
        assertThat(isr.values().stream().allMatch(lai -> lai.getLeader() == 1)).isTrue();
    }

    @Test
    public void fetchTopicConfigShouldRetrieveTheProperConfig() throws Exception {

        kafka.createTopic(TopicConfig.forTopic("test-topic")
            .with("min.insync.replicas", "1")
            .build());

        delay(3);

        Properties topicConfig = kafka.fetchTopicConfig("test-topic");

        assertThat(topicConfig.getProperty("min.insync.replicas")).isEqualTo("1");
    }

    @Test(expected = RuntimeException.class)
    public void fetchTopicConfigShouldThrowRuntimeExceptionIfTopicDoesNotExist() throws Exception {

        kafka.fetchTopicConfig(UUID.randomUUID().toString());
    }
}
