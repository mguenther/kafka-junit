package net.mguenther.kafka.junit.provider;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.KeyValueMetadata;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.RecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class DefaultRecordConsumer implements RecordConsumer {

    private final String bootstrapServers;

    @Override
    public <V> List<V> readValues(final ReadKeyValues<String, V> readRequest) throws InterruptedException {
        final List<KeyValue<String, V>> kvs = read(readRequest);
        return Collections.unmodifiableList(kvs.stream().map(KeyValue::getValue).collect(Collectors.toList()));
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) throws InterruptedException {
        final List<KeyValue<K, V>> consumedRecords = new ArrayList<>();
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(effectiveConsumerProps(readRequest.getConsumerProps()));
        final int pollIntervalMillis = 100;
        final int limit = readRequest.getLimit();
        final Predicate<K> filterOnKeys = readRequest.getFilterOnKeys();
        final Predicate<V> filterOnValues = readRequest.getFilterOnValues();
        final Predicate<Headers> filterOnHeaders = readRequest.getFilterOnHeaders();
        consumer.subscribe(Collections.singletonList(readRequest.getTopic()));
        int totalPollTimeMillis = 0;
        boolean assignmentsReady = false;
        while (totalPollTimeMillis < readRequest.getMaxTotalPollTimeMillis() && continueConsuming(consumedRecords.size(), limit)) {
            final ConsumerRecords<K, V> records = consumer.poll(pollIntervalMillis);
            // Kafka exchanges partition assignments and revocations between broker and consumers via the
            // poll mechanism. So, if we attempt to seek to an dedicated offset for a topic-partition for
            // which the consumer has not been notified of its assignment yet (there was no previous call
            // to poll for instance), then the seek operation will fail. Hence, we call poll and then check
            // if we have to seek. If did indeed seek, we omit the records returned by the first poll and
            // do it over. If we did not seek then everything is back to normal and we process the already
            // obtained records regularly.
            if (!assignmentsReady) {
                assignmentsReady = true;
                if (seekIfNecessary(readRequest, consumer)) continue;
            }
            for (ConsumerRecord<K, V> record : records) {
                if (filterOnKeys.test(record.key()) && filterOnValues.test(record.value()) && filterOnHeaders.test(record.headers())) {
                    final KeyValue<K, V> kv = readRequest.isIncludeMetadata() ?
                            new KeyValue<>(record.key(), record.value(), record.headers(), new KeyValueMetadata(record.topic(), record.partition(), record.offset())) :
                            new KeyValue<>(record.key(), record.value(), record.headers());

                    consumedRecords.add(kv);
                }
            }
            totalPollTimeMillis += pollIntervalMillis;
        }
        consumer.commitSync();
        consumer.close();
        return Collections.unmodifiableList(consumedRecords);
    }

    private <K, V> boolean seekIfNecessary(final ReadKeyValues<K, V> readRequest, final KafkaConsumer<K, V> consumer) {
        final boolean shouldSeek = readRequest.getSeekTo().size() > 0;
        if (shouldSeek) {
            readRequest
                    .getSeekTo()
                    .keySet()
                    .stream()
                    .map(partition -> new TopicPartition(readRequest.getTopic(), partition))
                    .peek(topicPartition -> log.info("Seeking to offset {} of topic-partition {}.", readRequest.getSeekTo().get(topicPartition.partition()), topicPartition))
                    .forEach(topicPartition -> consumer.seek(topicPartition, readRequest.getSeekTo().get(topicPartition.partition())));
        }
        return shouldSeek;
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    @Override
    public <V> List<V> observeValues(final ObserveKeyValues<String, V> observeRequest) throws InterruptedException {
        final List<V> totalConsumedRecords = new ArrayList<>(observeRequest.getExpected());
        final long startNanos = System.nanoTime();
        final ReadKeyValues<String, V> initialReadRequest = withPartitionSeekForReadValues(observeRequest);
        final ReadKeyValues<String, V> subsequentReadRequests = withoutPartitionSeekForReadValues(observeRequest);
        boolean firstRequest = true;
        while (true) {
            final List<V> consumedRecords = firstRequest ?
                    readValues(initialReadRequest) :
                    readValues(subsequentReadRequests);
            totalConsumedRecords.addAll(consumedRecords);
            if (firstRequest) firstRequest = false;
            if (totalConsumedRecords.size() >= observeRequest.getExpected()) break;
            if (System.nanoTime() > startNanos + TimeUnit.MILLISECONDS.toNanos(observeRequest.getObservationTimeMillis())) {
                final String message = String.format("Expected %s records, but consumed only %s records before ran " +
                                "into timeout (%s ms).",
                        observeRequest.getExpected(),
                        totalConsumedRecords.size(),
                        observeRequest.getObservationTimeMillis());
                throw new AssertionError(message);
            }
            Thread.sleep(Math.min(observeRequest.getObservationTimeMillis(), 100));
        }
        return Collections.unmodifiableList(totalConsumedRecords);
    }

    private <V> ReadKeyValues<String, V> withPartitionSeekForReadValues(final ObserveKeyValues<String, V> observeRequest) {
        return toReadValuesRequest(observeRequest).seekTo(observeRequest.getSeekTo()).build();
    }

    private <V> ReadKeyValues<String, V> withoutPartitionSeekForReadValues(final ObserveKeyValues<String, V> observeRequest) {
        return toReadValuesRequest(observeRequest).build();
    }

    private <V> ReadKeyValues.ReadKeyValuesBuilder<String, V> toReadValuesRequest(final ObserveKeyValues<String, V> observeRequest) {
        return ReadKeyValues.from(observeRequest.getTopic(), observeRequest.getClazzOfV())
                .withAll(observeRequest.getConsumerProps())
                .withLimit(observeRequest.getExpected())
                .withMetadata(false)
                .filterOnKeys(observeRequest.getFilterOnKeys())
                .filterOnValues(observeRequest.getFilterOnValues())
                .filterOnHeaders(observeRequest.getFilterOnHeaders())
                .with(ConsumerConfig.GROUP_ID_CONFIG, observeRequest.getConsumerProps().getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Override
    public <K, V> List<KeyValue<K, V>> observe(final ObserveKeyValues<K, V> observeRequest) throws InterruptedException {
        final List<KeyValue<K, V>> totalConsumedRecords = new ArrayList<>(observeRequest.getExpected());
        final long startNanos = System.nanoTime();
        final ReadKeyValues<K, V> initialReadRequest = withPartitionSeek(observeRequest);
        final ReadKeyValues<K, V> subsequentReadRequests = withoutPartitionSeek(observeRequest);
        boolean firstRequest = true;
        while (true) {
            final List<KeyValue<K, V>> consumedRecords = firstRequest ?
                    read(initialReadRequest) :
                    read(subsequentReadRequests);
            totalConsumedRecords.addAll(consumedRecords);
            if (firstRequest) firstRequest = false;
            if (totalConsumedRecords.size() >= observeRequest.getExpected()) break;
            if (System.nanoTime() > startNanos + TimeUnit.MILLISECONDS.toNanos(observeRequest.getObservationTimeMillis())) {
                final String message = String.format("Expected %s records, but consumed only %s records before ran " +
                                "into timeout (%s ms).",
                        observeRequest.getExpected(),
                        totalConsumedRecords.size(),
                        observeRequest.getObservationTimeMillis());
                throw new AssertionError(message);
            }
            Thread.sleep(Math.min(observeRequest.getObservationTimeMillis(), 100));
        }
        return Collections.unmodifiableList(totalConsumedRecords);
    }

    private <K, V> ReadKeyValues<K, V> withPartitionSeek(final ObserveKeyValues<K, V> observeRequest) {
        return toReadKeyValuesRequest(observeRequest).seekTo(observeRequest.getSeekTo()).build();
    }

    private <K, V> ReadKeyValues<K, V> withoutPartitionSeek(final ObserveKeyValues<K, V> observeRequest) {
        return toReadKeyValuesRequest(observeRequest).build();
    }

    private <K, V> ReadKeyValues.ReadKeyValuesBuilder<K, V> toReadKeyValuesRequest(final ObserveKeyValues<K, V> observeRequest) {
        return ReadKeyValues.from(observeRequest.getTopic(), observeRequest.getClazzOfK(), observeRequest.getClazzOfV())
                .withAll(observeRequest.getConsumerProps())
                .withLimit(observeRequest.getExpected())
                .withMetadata(observeRequest.isIncludeMetadata())
                .filterOnKeys(observeRequest.getFilterOnKeys())
                .filterOnValues(observeRequest.getFilterOnValues())
                .filterOnHeaders(observeRequest.getFilterOnHeaders())
                .with(ConsumerConfig.GROUP_ID_CONFIG, observeRequest.getConsumerProps().getProperty(ConsumerConfig.GROUP_ID_CONFIG));
    }

    private Properties effectiveConsumerProps(final Properties originalConsumerProps) {
        final Properties effectiveConsumerProps = new Properties();
        effectiveConsumerProps.putAll(originalConsumerProps);
        effectiveConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return effectiveConsumerProps;
    }
}
