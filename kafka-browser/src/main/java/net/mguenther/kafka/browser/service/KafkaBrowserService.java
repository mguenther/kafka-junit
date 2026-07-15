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
        return readRecords(topic, partition, offset, limit, null, null);
    }

    /**
     * Reads records from a specific topic and partition with custom deserializers.
     */
    @SuppressWarnings("unchecked")
    public List<KeyValue<String, String>> readRecords(String topic, int partition, long offset, int limit,
                                                      String keyDeserializer, String valueDeserializer) {
        try {
            ReadKeyValues<Object, Object> request = ReadKeyValues.from(topic, Object.class, Object.class)
                    .seekTo(partition, offset)
                    .withLimit(limit)
                    .includeMetadata()
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClassName(keyDeserializer))
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClassName(valueDeserializer))
                    .withMaxTotalPollTime(5, TimeUnit.SECONDS)
                    .build();

            List<KeyValue<Object, Object>> raw = consumer.read(request);

            return raw.stream()
                    .map(kv -> {
                        String key = kv.getKey() != null ? kv.getKey().toString() : null;
                        String value = kv.getValue() != null ? kv.getValue().toString() : null;
                        return new KeyValue<>(key, value, kv.getHeaders(),
                                kv.getMetadata().orElse(null));
                    })
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while reading records from topic {}", topic, e);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Failed to read records from topic {} with deserializers key={}, value={}",
                    topic, keyDeserializer, valueDeserializer, e);
            throw new DeserializationException("Unable to deserialize records due to mismatching key/value deserializers.", e);
        }
    }
    private String deserializerClassName(String shortName) {
        if (shortName == null || shortName.isEmpty()) return "org.apache.kafka.common.serialization.StringDeserializer";
        return switch (shortName) {
            case "String" -> "org.apache.kafka.common.serialization.StringDeserializer";
            case "Double" -> "org.apache.kafka.common.serialization.DoubleDeserializer";
            case "Float" -> "org.apache.kafka.common.serialization.FloatDeserializer";
            case "Integer" -> "org.apache.kafka.common.serialization.IntegerDeserializer";
            case "Long" -> "org.apache.kafka.common.serialization.LongDeserializer";
            case "Short" -> "org.apache.kafka.common.serialization.ShortDeserializer";
            case "UUID" -> "org.apache.kafka.common.serialization.UUIDDeserializer";
            default -> "org.apache.kafka.common.serialization.StringDeserializer";
        };
    }

    /**
     * Reads records from all partitions of a topic (no specific seek).
     * Returns up to {@code limit} records.
     */
    public List<KeyValue<String, String>> readRecords(String topic, int limit) {
        return readRecordsAllPartitions(topic, limit, null, null);
    }

    /**
     * Reads records from all partitions of a topic with custom deserializers.
     * Returns up to {@code limit} records.
     */
    @SuppressWarnings("unchecked")
    public List<KeyValue<String, String>> readRecordsAllPartitions(String topic, int limit,
                                                                    String keyDeserializer, String valueDeserializer) {
        try {
            ReadKeyValues<Object, Object> request = ReadKeyValues.from(topic, Object.class, Object.class)
                    .withLimit(limit)
                    .includeMetadata()
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClassName(keyDeserializer))
                    .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClassName(valueDeserializer))
                    .withMaxTotalPollTime(5, TimeUnit.SECONDS)
                    .build();

            List<KeyValue<Object, Object>> raw = consumer.read(request);
            return raw.stream()
                    .map(kv -> new KeyValue<>(
                            kv.getKey() != null ? kv.getKey().toString() : null,
                            kv.getValue() != null ? kv.getValue().toString() : null,
                            kv.getHeaders(),
                            kv.getMetadata().orElse(null)))
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while reading records from topic {}", topic, e);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.error("Failed to read records from all partitions of topic {}", topic, e);
            throw new DeserializationException("Unable to deserialize records due to mismatching key/value deserializers.", e);
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
        return produceRecord(topic, key, value, null, null, null);
    }

    /**
     * Produces a single key-value record to a topic with custom serializers and headers.
     * Converts the string input to the appropriate type based on the serializer class.
     */
    public RecordMetadata produceRecord(String topic, String key, String value,
                                        String keySerializer, String valueSerializer,
                                        java.util.Map<String, String> headers) {
        try {
            Object typedKey = convertToType(key, keySerializer);
            Object typedValue = convertToType(value, valueSerializer);

            @SuppressWarnings("unchecked")
            KeyValue<Object, Object> record = new KeyValue<>(typedKey, typedValue);
            if (headers != null && !headers.isEmpty()) {
                for (var entry : headers.entrySet()) {
                    record.addHeader(entry.getKey(), entry.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }

            @SuppressWarnings("unchecked")
            SendKeyValues.SendKeyValuesBuilder<Object, Object> builder = SendKeyValues
                    .to(topic, Collections.singletonList(record));
            if (keySerializer != null && !keySerializer.isEmpty()) {
                builder.with(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
            }
            if (valueSerializer != null && !valueSerializer.isEmpty()) {
                builder.with(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
            }
            List<RecordMetadata> metadata = producer.send(builder.build());
            return metadata.isEmpty() ? null : metadata.get(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while producing record to topic {}", topic, e);
            return null;
        } catch (Exception e) {
            LOG.error("Failed to produce record to topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Failed to produce record: " + e.getMessage(), e);
        }
    }

    /**
     * Converts a string input to the appropriate Java type based on the serializer class name.
     */
    private Object convertToType(String input, String serializerClass) {
        if (serializerClass == null || serializerClass.isEmpty() || serializerClass.contains("StringSerializer")) {
            return input;
        }
        if (serializerClass.contains("UUIDSerializer")) {
            return java.util.UUID.fromString(input);
        }
        if (serializerClass.contains("IntegerSerializer")) {
            return Integer.parseInt(input);
        }
        if (serializerClass.contains("LongSerializer")) {
            return Long.parseLong(input);
        }
        if (serializerClass.contains("DoubleSerializer")) {
            return Double.parseDouble(input);
        }
        if (serializerClass.contains("FloatSerializer")) {
            return Float.parseFloat(input);
        }
        if (serializerClass.contains("ShortSerializer")) {
            return Short.parseShort(input);
        }
        if (serializerClass.contains("VoidSerializer")) {
            return null;
        }
        return input;
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

    /**
     * Retrieves cluster information: cluster ID, brokers, controller.
     */
    public ClusterInfo getClusterInfo() {
        Properties props = adminProps();
        try (AdminClient client = AdminClient.create(props)) {
            org.apache.kafka.clients.admin.DescribeClusterResult result = client.describeCluster();
            String clusterId = result.clusterId().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            org.apache.kafka.common.Node controller = result.controller().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            java.util.Collection<org.apache.kafka.common.Node> nodes = result.nodes().get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            List<String> brokers = new ArrayList<>();
            for (org.apache.kafka.common.Node node : nodes) {
                brokers.add(node.id() + " @ " + node.host() + ":" + node.port());
            }

            String controllerInfo = controller != null
                    ? controller.id() + " @ " + controller.host() + ":" + controller.port()
                    : "unknown";

            int totalTopics = listTopics().size();

            // Detect Kafka version via inter.broker.protocol.version from any broker
            String kafkaVersion = "unknown";
            if (!nodes.isEmpty()) {
                org.apache.kafka.common.Node anyBroker = nodes.iterator().next();
                org.apache.kafka.common.config.ConfigResource brokerResource =
                        new org.apache.kafka.common.config.ConfigResource(
                                org.apache.kafka.common.config.ConfigResource.Type.BROKER,
                                String.valueOf(anyBroker.id()));
                try {
                    org.apache.kafka.clients.admin.Config brokerConfig = client
                            .describeConfigs(Collections.singletonList(brokerResource))
                            .all()
                            .get(ADMIN_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                            .get(brokerResource);
                    org.apache.kafka.clients.admin.ConfigEntry entry = brokerConfig.get("inter.broker.protocol.version");
                    if (entry != null && entry.value() != null) {
                        kafkaVersion = entry.value();
                    }
                } catch (Exception ex) {
                    LOG.warn("Could not fetch inter.broker.protocol.version: {}", ex.getMessage());
                }
            }

            return new ClusterInfo(clusterId, brokers, controllerInfo, totalTopics, kafkaVersion);
        } catch (Exception e) {
            LOG.error("Failed to get cluster info from {}", bootstrapServers, e);
            return new ClusterInfo("unavailable", Collections.emptyList(), "unavailable", 0, "unknown");
        }
    }

    /**
     * Holds cluster metadata.
     */
    public record ClusterInfo(String clusterId, List<String> brokers, String controller, int totalTopics, String kafkaVersion) {}

    /**
     * Creates a new topic using the given TopicConfig.
     */
    public void createTopic(net.mguenther.kafka.junit.TopicConfig topicConfig) {
        net.mguenther.kafka.junit.provider.DefaultTopicManager topicManager =
                new net.mguenther.kafka.junit.provider.DefaultTopicManager(bootstrapServers);
        topicManager.createTopic(topicConfig);
    }

    /**
     * Fetches detailed metadata for a topic: configuration properties and partition/ISR info.
     */
    public TopicDetails fetchTopicDetails(String topic) {
        try {
            net.mguenther.kafka.junit.provider.DefaultTopicManager topicManager =
                    new net.mguenther.kafka.junit.provider.DefaultTopicManager(bootstrapServers);
            java.util.Properties config = topicManager.fetchTopicConfig(topic);
            java.util.Map<Integer, net.mguenther.kafka.junit.LeaderAndIsr> leaderAndIsr = topicManager.fetchLeaderAndIsr(topic);
            Map<Integer, Long> endOffsets = getEndOffsets(topic);
            Map<Integer, Long> beginOffsets = getBeginningOffsets(topic);

            long totalMessages = 0;
            for (Map.Entry<Integer, Long> entry : endOffsets.entrySet()) {
                long begin = beginOffsets.getOrDefault(entry.getKey(), 0L);
                totalMessages += entry.getValue() - begin;
            }

            return new TopicDetails(config, leaderAndIsr, totalMessages);
        } catch (Exception e) {
            LOG.error("Failed to fetch topic details for {}", topic, e);
            return new TopicDetails(new java.util.Properties(), Collections.emptyMap(), 0);
        }
    }

    /**
     * Holds detailed topic metadata.
     */
    public record TopicDetails(
            java.util.Properties config,
            java.util.Map<Integer, net.mguenther.kafka.junit.LeaderAndIsr> partitions,
            long approximateMessageCount
    ) {}

    private Properties adminProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", "kafka-browser-admin");
        props.putAll(additionalProps);
        return props;
    }
}
