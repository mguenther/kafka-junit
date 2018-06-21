package net.mguenther.kafka.junit.provider;

import lombok.RequiredArgsConstructor;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.RecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class DefaultRecordConsumer implements RecordConsumer {

    private final String bootstrapServers;

    @Override
    public <V> List<V> readValues(final ReadKeyValues<String, V> readRequest) {
        final List<KeyValue<String, V>> kvs = read(readRequest);
        return Collections.unmodifiableList(kvs.stream().map(KeyValue::getValue).collect(Collectors.toList()));
    }

    @Override
    public <K, V> List<KeyValue<K, V>> read(final ReadKeyValues<K, V> readRequest) {
        final List<KeyValue<K, V>> consumedRecords = new ArrayList<>();
        final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(effectiveConsumerProps(readRequest.getConsumerProps()));
        final int pollIntervalMillis = 100;
        final int limit = readRequest.getLimit();
        final Predicate<K> filterOnKeys = readRequest.getFilterOnKeys();
        final Predicate<V> filterOnValues = readRequest.getFilterOnValues();
        consumer.subscribe(Collections.singletonList(readRequest.getTopic()));
        int totalPollTimeMillis = 0;
        while (totalPollTimeMillis < readRequest.getMaxTotalPollTimeMillis() && continueConsuming(consumedRecords.size(), limit)) {
            final ConsumerRecords<K, V> records = consumer.poll(pollIntervalMillis);
            for (ConsumerRecord<K, V> record : records) {
                if (filterOnKeys.test(record.key()) && filterOnValues.test(record.value())) {
                    consumedRecords.add(new KeyValue<>(record.key(), record.value(), record.headers()));
                }
            }
            totalPollTimeMillis += pollIntervalMillis;
        }
        consumer.commitSync();
        consumer.close();
        return Collections.unmodifiableList(consumedRecords);
    }

    private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
        return maxMessages <= 0 || messagesConsumed < maxMessages;
    }

    @Override
    public <V> List<V> observeValues(final ObserveKeyValues<String, V> observeRequest) throws InterruptedException {
        final List<V> totalConsumedRecords = new ArrayList<>(observeRequest.getExpected());
        final long startNanos = System.nanoTime();
        while (true) {
            final ReadKeyValues<String, V> readRequest = ReadKeyValues.from(observeRequest.getTopic(), observeRequest.getClazzOfV())
                    .withAll(observeRequest.getConsumerProps())
                    .withLimit(observeRequest.getExpected())
                    .filterOnKeys(observeRequest.getFilterOnKeys())
                    .filterOnValues(observeRequest.getFilterOnValues())
                    .build();
            final List<V> consumedRecords = readValues(readRequest);
            totalConsumedRecords.addAll(consumedRecords);
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

    @Override
    public <K, V> List<KeyValue<K, V>> observe(final ObserveKeyValues<K, V> observeRequest) throws InterruptedException {
        final List<KeyValue<K, V>> totalConsumedRecords = new ArrayList<>(observeRequest.getExpected());
        final long startNanos = System.nanoTime();
        final ReadKeyValues<K, V> readRequest = ReadKeyValues.from(observeRequest.getTopic(), observeRequest.getClazzOfK(), observeRequest.getClazzOfV())
                .withAll(observeRequest.getConsumerProps())
                .withLimit(observeRequest.getExpected())
                .filterOnKeys(observeRequest.getFilterOnKeys())
                .filterOnValues(observeRequest.getFilterOnValues())
                .build();
        while (true) {
            final List<KeyValue<K, V>> consumedRecords = read(readRequest);
            totalConsumedRecords.addAll(consumedRecords);
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

    private Properties effectiveConsumerProps(final Properties originalConsumerProps) {
        final Properties effectiveConsumerProps = new Properties();
        effectiveConsumerProps.putAll(originalConsumerProps);
        effectiveConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return effectiveConsumerProps;
    }
}
