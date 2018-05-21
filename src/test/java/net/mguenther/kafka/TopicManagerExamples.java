package net.mguenther.kafka;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static net.mguenther.kafka.EmbeddedKafkaClusterConfig.useDefaults;
import static net.mguenther.kafka.EmbeddedKafkaClusterRule.provisionWith;

@Ignore
public class TopicManagerExamples {

    @Rule
    public EmbeddedKafkaClusterRule cluster = provisionWith(useDefaults());

    @Test
    public void manageTopics() {

        cluster.createTopic(TopicConfig.forTopic("test-topic").useDefaults());
        cluster.exists("test-topic");
        cluster.deleteTopic("test-topic");
    }
}
