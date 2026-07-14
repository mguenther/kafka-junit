package net.mguenther.kafka.browser.service;

import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import net.mguenther.kafka.junit.provider.DefaultRecordProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Service layer that bridges the JavaFX UI to kafka-junit's client infrastructure.
 * Provides topic listing, record reading (with seek/pagination), and record producing.
 */
public class KafkaBrowserService {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaBrowserService.class);
    private static final int ADMIN_TIMEOUT_MS = 10_000;

    private final String bootstrapServers;
    private final Properties additionalProps;
    private final DefaultRecordConsumer consumer;
    private final DefaultRecordProducer producer;

    public KafkaBrowserService(Workspace workspace, Environment environment) {
        this.bootstrapServers = workspace.getBootstrapServers();
        this.additionalProps = new Properties();
        if (environment != null) {
            environment.getParameters().forEach(additionalProps::put);
        }
        this.consumer = new DefaultRecordConsumer(bootstrapServers);
        this.producer = new DefaultRecordProducer(bootstrapServers);
    }

    /**
     * Lists all non-internal topics on the cluster with their partition and replica counts.
     */
    public List<TopicInfo> listTopics() {
        Properties props = adminProps();
        try (AdminClient client = AdminClient.create(props)) {
            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(false);

            Collection<TopicListing> listings = client.listTopics(options)
                    .listings()
                    .get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<String> topicNames = listings.stream()
                    .map(TopicListing::name)
                    .collect(Collectors.toList());

            if (topicNames.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeTopicsResult describeResult = client.describeTopics(topicNames);
            Map<String, TopicDescription> descriptions = describeResult
                    .allTopicNames()
                    .get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<TopicInfo> result = new ArrayList<>();
            for (TopicDescription desc : descriptions.values()) {
                int partitions = desc.partitions().size();
                int replicas = partitions > 0
                        ? desc.partitions().get(0).replicas().size()
                        : 0;
                result.add(new TopicInfo(desc.name(), partitions, replicas));
            }
            result.sort((a, b) -> a.getName().compareToIgnoreCase(b.getName()));
            return result;
        } catch (Exception e) {
            LOG.error("Failed to list topics from {}", bootstrapServers, e);
            return Collections.emptyList();
        }
    }

    /**
     * Reads records from a specific topic and partition, starting at a given offset.
     * Returns up to {@code limit} records.
     */
    public List<KeyValue<String, String>> readRecords(String topic, int partition, long offset, int limit) {
        try {
            ReadKeyValues<String, String> request = ReadKeyValues.from(topic)
                    .seekTo(partition, offset)
                    .withLimit(limit)
                    .includeMetadata()
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withMaxTotalPollTime(5, TimeUnit.SECONDS)
                    .build();
            return consumer.read(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while reading records from topic {}", topic, e);
            return Collections.emptyList();
        }
    }

    /**
     * Reads records from all partitions of a topic (no specific seek).
     * Returns up to {@code limit} records.
     */
    public List<KeyValue<String, String>> readRecords(String topic, int limit) {
        try {
            ReadKeyValues<String, String> request = ReadKeyValues.from(topic)
                    .withLimit(limit)
                    .includeMetadata()
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withMaxTotalPollTime(5, TimeUnit.SECONDS)
                    .build();
            return consumer.read(request);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while reading records from topic {}", topic, e);
            return Collections.emptyList();
        }
    }

    /**
     * Searches records in a topic by filtering key or value content.
     */
    public List<KeyValue<String, String>> searchRecords(String topic, String searchTerm, int limit) {
        try {
            ReadKeyValues<String, String> request = ReadKeyValues.from(topic)
                    .withLimit(limit)
                    .includeMetadata()
                    .filterOnKeys(key -> key != null && key.contains(searchTerm))
                    .filterOnValues(value -> value != null && value.contains(searchTerm))
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withMaxTotalPollTime(10, TimeUnit.SECONDS)
                    .build();
            // Note: kafka-junit's filter is AND-based (key AND value must match).
            // For a topic browser we want OR semantics, so we read all and filter ourselves.
            ReadKeyValues<String, String> readAll = ReadKeyValues.from(topic)
                    .includeMetadata()
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withMaxTotalPollTime(10, TimeUnit.SECONDS)
                    .build();
            List<KeyValue<String, String>> all = consumer.read(readAll);
            return all.stream()
                    .filter(kv -> (kv.getKey() != null && kv.getKey().contains(searchTerm))
                            || (kv.getValue() != null && kv.getValue().contains(searchTerm)))
                    .limit(limit)
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while searching records in topic {}", topic, e);
            return Collections.emptyList();
        }
    }

    /**
     * Produces a single key-value record to a topic.
     */
    public RecordMetadata produceRecord(String topic, String key, String value) {
        try {
            KeyValue<String, String> record = new KeyValue<>(key, value);
            SendKeyValues<String, String> request = SendKeyValues
                    .to(topic, Collections.singletonList(record))
                    .build();
            List<RecordMetadata> metadata = producer.send(request);
            return metadata.isEmpty() ? null : metadata.get(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while producing record to topic {}", topic, e);
            return null;
        }
    }

    /**
     * Gets the end offsets (latest offset) for each partition of a topic.
     * Useful for pagination: knowing how many records exist.
     */
    public Map<Integer, Long> getEndOffsets(String topic) {
        Properties props = adminProps();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-browser-offsets-" + System.currentTimeMillis());

        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer =
                     new org.apache.kafka.clients.consumer.KafkaConsumer<>(props)) {
            List<org.apache.kafka.common.TopicPartition> partitions = kafkaConsumer
                    .partitionsFor(topic)
                    .stream()
                    .map(pi -> new org.apache.kafka.common.TopicPartition(topic, pi.partition()))
                    .collect(Collectors.toList());
            Map<org.apache.kafka.common.TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
            return endOffsets.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().partition(),
                            Map.Entry::getValue
                    ));
        } catch (Exception e) {
            LOG.error("Failed to get end offsets for topic {}", topic, e);
            return Collections.emptyMap();
        }
    }

    /**
     * Gets the beginning offsets for each partition of a topic.
     */
    public Map<Integer, Long> getBeginningOffsets(String topic) {
        Properties props = adminProps();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-browser-offsets-" + System.currentTimeMillis());

        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer =
                     new org.apache.kafka.clients.consumer.KafkaConsumer<>(props)) {
            List<org.apache.kafka.common.TopicPartition> partitions = kafkaConsumer
                    .partitionsFor(topic)
                    .stream()
                    .map(pi -> new org.apache.kafka.common.TopicPartition(topic, pi.partition()))
                    .collect(Collectors.toList());
            Map<org.apache.kafka.common.TopicPartition, Long> beginOffsets = kafkaConsumer.beginningOffsets(partitions);
            return beginOffsets.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().partition(),
                            Map.Entry::getValue
                    ));
        } catch (Exception e) {
            LOG.error("Failed to get beginning offsets for topic {}", topic, e);
            return Collections.emptyMap();
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "kafka-browser-admin");
        props.putAll(additionalProps);
        return props;
    }
}
