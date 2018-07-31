package net.mguenther.kafka.junit.provider;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.api.LeaderAndIsr;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.TopicConfig;
import net.mguenther.kafka.junit.TopicManager;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import scala.Option;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@RequiredArgsConstructor
public class DefaultTopicManager implements TopicManager {

    private final String zkConnectString;

    @Override
    public void createTopic(final TopicConfig topicConfig) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = get();
            AdminUtils.createTopic(
                    zkUtils,
                    topicConfig.getTopic(),
                    topicConfig.getNumberOfPartitions(),
                    topicConfig.getNumberOfReplicas(),
                    topicConfig.getProperties(),
                    RackAwareMode.Enforced$.MODULE$);
            log.info("Created topic '{}' with settings {}.", topicConfig.getTopic(), topicConfig);
        } catch (IllegalArgumentException | InvalidTopicException e) {
            throw new RuntimeException("Invalid topic settings.", e);
        } catch (TopicExistsException e) {
            throw new RuntimeException(String.format("The topic '%s' already exists.", topicConfig.getTopic()), e);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to create topic '%s'.", topicConfig.getTopic()), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Override
    public void deleteTopic(final String topic) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = get();
            AdminUtils.deleteTopic(zkUtils, topic);
            log.info("Marked topic '{}' for deletion.", topic);
        } catch (TopicAlreadyMarkedForDeletionException e) {
            throw new RuntimeException(String.format("The topic '%s' has already been marked for deletion.", topic), e);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to delete topic '%s'.", topic), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Override
    public boolean exists(final String topic) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = get();
            return AdminUtils.topicExists(zkUtils, topic);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Unable to query the state of topic '%s'.", topic), e);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Override
    public Map<Integer, LeaderAndIsr> fetchLeaderAndIsr(final String topic) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = get();
            final Map<Integer, LeaderAndIsr> leaderAndIsrForTopic = new HashMap<>();
            final Option<Object> partitionsOpt = zkUtils.getTopicPartitionCount(topic);
            final int partitions = partitionsOpt.isEmpty() ? 0 : (Integer) partitionsOpt.get();
            for (int partition = 0; partition < partitions; partition++) {
                final Option<LeaderAndIsr> leaderAndIsrOption = zkUtils.getLeaderAndIsrForPartition(topic, partition);
                if (leaderAndIsrOption.isEmpty()) log.info("Unable to retrieve leader and ISR for topic-partition {}-{}.", topic, partition);
                else leaderAndIsrForTopic.put(partition, leaderAndIsrOption.get());
            }
            return Collections.unmodifiableMap(leaderAndIsrForTopic);
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    @Override
    public Properties fetchTopicConfig(final String topic) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = get();
            final Option<Properties> topicConfigOpt = AdminUtils.fetchAllTopicConfigs(zkUtils).get(topic);
            if (topicConfigOpt.isEmpty()) throw new RuntimeException(String.format("Unable to retrieve configuration for topic %s.", topic));
            return topicConfigOpt.get();
        } finally {
            if (zkUtils != null) {
                zkUtils.close();
            }
        }
    }

    private ZkUtils get() {
        final ZkClient zkClient = new ZkClient(
                zkConnectString,
                10_000,
                8_000,
                ZKStringSerializer$.MODULE$);
        final ZkConnection zkConnection = new ZkConnection(zkConnectString);
        return new ZkUtils(zkClient, zkConnection, false);
    }
}
