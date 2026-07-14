package net.mguenther.kafka.junit.provider;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.LeaderAndIsr;
import net.mguenther.kafka.junit.TopicConfig;
import net.mguenther.kafka.junit.TopicManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.util.TopicAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@RequiredArgsConstructor
public class DefaultTopicManager implements TopicManager {

    private static final int TIMEOUT_IN_MILLIS = 10_000;

    private final Properties props;

    public DefaultTopicManager(final String bootstrapServers) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "kafka-junit-admin-client");
        this.props = props;
    }

    @Override
    public void createTopic(final TopicConfig config) {
        try (AdminClient client = AdminClient.create(props)) {
            final NewTopic newTopic = TopicAdmin.defineTopic(config.getTopic())
                .partitions(config.getNumberOfPartitions())
                .replicationFactor((short) config.getNumberOfReplicas())
                .config(config.getPropertiesMap())
                .build();
            client.createTopics(Collections.singletonList(newTopic))
                .all()
                .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // due to interface compatibility, we cannot re-throw the InterruptedException,
            // but will restore the interrupted flag. we also need to throw a RuntimeException
            // so that the test case fails properly
            Thread.currentThread().interrupt();
            final String message = "Caught an unexpected InterruptedException while trying to create topic '%s'.";
            throw new RuntimeException(String.format(message, config.getTopic()), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                final String message = "The topic '%s' already exists.";
                throw new RuntimeException(String.format(message, config.getTopic()), e.getCause());
            } else {
                final String message = "Unable to create topic '%s'.";
                throw new RuntimeException(String.format(message, config.getTopic()), e.getCause());
            }
        } catch (TimeoutException e) {
            final String message = "A timeout occurred while trying to create topic '%s'.";
            throw new RuntimeException(String.format(message, config.getTopic()), e);
        }
    }

    @Override
    public void deleteTopic(final String topic) {
        try (AdminClient client = AdminClient.create(props)) {
            client
                .deleteTopics(Collections.singletonList(topic))
                .all()
                .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // due to interface compatibility, we cannot re-throw the InterruptedException,
            // but will restore the interrupted flag. we also need to throw a RuntimeException
            // so that the test case fails properly
            Thread.currentThread().interrupt();
            final String message = "Caught an unexpected InterruptedException while trying to delete topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        } catch (ExecutionException e) {
            final String message = "Unable to delete the topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e.getCause());
        } catch (TimeoutException e) {
            final String message = "A timeout occurred while trying to delete topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        }
    }

    @Override
    public boolean exists(final String topic) {
        boolean exists = false;
        try (AdminClient client = AdminClient.create(props)) {
            exists = getTopicNames(client).anyMatch(existingTopic -> existingTopic.equals(topic));
        } catch (InterruptedException e) {
            // due to interface compatibility, we cannot re-throw the InterruptedException,
            // but will restore the interrupted flag. we also need to throw a RuntimeException
            // so that the test case fails properly
            Thread.currentThread().interrupt();
            final String message = "Caught an unexpected InterruptedException while trying to determine if topic '%s' exists.";
            throw new RuntimeException(String.format(message, topic), e);
        } catch (ExecutionException e) {
            final String message = "Unable to query the state of topic '%s'";
            throw new RuntimeException(String.format(message, topic), e.getCause());
        } catch (TimeoutException e) {
            final String message = "A timeout occurred while trying to determine if topic '%s' exists.";
            throw new RuntimeException(String.format(message, topic), e);
        }
        return exists;
    }

    private Stream<String> getTopicNames(final AdminClient client) throws InterruptedException, ExecutionException, TimeoutException {
        final ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(false);
        return client.listTopics(options)
            .listings()
            .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)
            .stream()
            .map(TopicListing::name);
    }

    @Override
    public Map<Integer, LeaderAndIsr> fetchLeaderAndIsr(final String topic) {
        // Use the describeTopics method. The TopicDescription provides information on the leader node and the ISR.
        final Map<Integer, LeaderAndIsr> leaderAndIsrByPartition = new HashMap<>();
        try (AdminClient client = AdminClient.create(props)) {
            final DescribeTopicsResult result = client.describeTopics(Collections.singletonList(topic));
            final TopicDescription topicDescription = result
                .allTopicNames()
                .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS)
                .get(topic);
            for (TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                final int partition = topicPartitionInfo.partition();
                final LeaderAndIsr leaderAndIsr = new LeaderAndIsr(
                    topicPartitionInfo.leader().id(),
                    topicPartitionInfo.isr().stream().map(Node::id).collect(Collectors.toSet()));
                leaderAndIsrByPartition.put(partition, leaderAndIsr);
            }
        } catch (InterruptedException e) {
            // due to interface compatibility, we cannot re-throw the InterruptedException,
            // but will restore the interrupted flag. we also need to throw a RuntimeException
            // so that the test case fails properly
            Thread.currentThread().interrupt();
            final String message = "Caught an unexpected InterruptedException while trying to fetch the leader and ISR for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        } catch (ExecutionException e) {
            final String message = "Unable to fetch the leader and ISR for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e.getCause());
        } catch (TimeoutException e) {
            final String message = "A timeout occurred while trying to fetch the leader and ISR for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        }
        return Collections.unmodifiableMap(leaderAndIsrByPartition);
    }

    @Override
    public Properties fetchTopicConfig(final String topic) {
        final Properties topicConfig = new Properties();
        try (AdminClient client = AdminClient.create(props)) {
            final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            final Map<ConfigResource, Config> configsByConfigResource = client.describeConfigs(Collections.singletonList(resource))
                .all()
                .get(TIMEOUT_IN_MILLIS, TimeUnit.MILLISECONDS);
            final Config config = configsByConfigResource.get(resource);
            for (ConfigEntry configEntry : config.entries()) {
                topicConfig.put(configEntry.name(), configEntry.value());
            }
        } catch (InterruptedException e) {
            // due to interface compatibility, we cannot re-throw the InterruptedException,
            // but will restore the interrupted flag. we also need to throw a RuntimeException
            // so that the test case fails properly
            Thread.currentThread().interrupt();
            final String message = "Caught an unexpected InterruptedException while trying to fetch the topic configuration for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        } catch (ExecutionException e) {
            final String message = "Unable to retrieve the topic configuration for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e.getCause());
        } catch (TimeoutException e) {
            final String message = "A timeout occurred while trying to fetch the topic configuration for topic '%s'.";
            throw new RuntimeException(String.format(message, topic), e);
        }
        return topicConfig;
    }
}
