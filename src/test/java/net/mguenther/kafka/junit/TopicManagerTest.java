package net.mguenther.kafka.junit;

import kafka.api.LeaderAndIsr;
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
    public EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

    @Test
    public void manageTopics() {

        cluster.createTopic(TopicConfig.forTopic("test-topic").useDefaults());

        assertThat(cluster.exists("test-topic")).isTrue();

        // the topic will not be deleted immediately, but "marked for deletion"
        // hence a check on exists would return "false" directly after deleting
        // the topic
        cluster.deleteTopic("test-topic");
    }

    @Test
    public void fetchLeaderAndIsrShouldRetrieveTheIsr() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .withNumberOfPartitions(5)
                .withNumberOfReplicas(1)
                .build());

        // it takes a couple of seconds until topic-partition assignments are there
        delay(5);

        Map<Integer, LeaderAndIsr> isr = cluster.fetchLeaderAndIsr("test-topic");

        assertThat(isr.size()).isEqualTo(5);
        assertThat(isr.values().stream().allMatch(lai -> lai.leader() == 1)).isTrue();
    }

    @Test
    public void fetchTopicConfigShouldRetrieveTheProperConfig() throws Exception {

        cluster.createTopic(TopicConfig.forTopic("test-topic")
                .with("min.insync.replicas", "1")
                .build());

        delay(3);

        Properties topicConfig = cluster.fetchTopicConfig("test-topic");

        assertThat(topicConfig.getProperty("min.insync.replicas")).isEqualTo("1");
    }

    @Test(expected = RuntimeException.class)
    public void fetchTopicConfigShouldThrowRuntimeExceptionIfTopicDoesNotExist() throws Exception {

        cluster.fetchTopicConfig(UUID.randomUUID().toString());
    }
}
