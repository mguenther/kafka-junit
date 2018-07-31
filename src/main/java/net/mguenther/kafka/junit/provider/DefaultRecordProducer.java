package net.mguenther.kafka.junit.provider;

import lombok.RequiredArgsConstructor;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.RecordProducer;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.SendKeyValuesTransactional;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.SendValuesTransactional;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class DefaultRecordProducer implements RecordProducer {

    private final String bootstrapServers;

    @Override
    public <V> List<RecordMetadata> send(final SendValues<V> sendRequest) throws InterruptedException {
        final Collection<KeyValue<String, V>> records = sendRequest.getValues()
                .stream()
                .map(value -> new KeyValue<>((String) null, value))
                .collect(Collectors.toList());
        final SendKeyValues<String, V> keyValueRequest = SendKeyValues
                .to(sendRequest.getTopic(), records)
                .withAll(sendRequest.getProducerProps())
                .build();
        return send(keyValueRequest);
    }

    @Override
    public <V> List<RecordMetadata> send(final SendValuesTransactional<V> sendRequest) throws InterruptedException {
        final Map<String, Collection<KeyValue<String, V>>> recordsPerTopic = new HashMap<>();
        for (String topic : sendRequest.getValuesPerTopic().keySet()) {
            final Collection<KeyValue<String, V>> records = sendRequest.getValuesPerTopic().get(topic)
                    .stream()
                    .map(value -> new KeyValue<>((String) null, value))
                    .collect(Collectors.toList());
            recordsPerTopic.put(topic, records);
        }
        final SendKeyValuesTransactional<String, V> keyValueRequest = SendKeyValuesTransactional.inTransaction(recordsPerTopic)
                .withAll(sendRequest.getProducerProps())
                .withFailTransaction(sendRequest.shouldFailTransaction())
                .build();
        return send(keyValueRequest);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValues<K, V> sendRequest) throws InterruptedException {
        final Producer<K, V> producer = new KafkaProducer<>(effectiveProducerProps(sendRequest.getProducerProps()));
        final List<RecordMetadata> metadata = new ArrayList<>(sendRequest.getRecords().size());
        try {
            for (KeyValue<K, V> record : sendRequest.getRecords()) {
                final ProducerRecord<K, V> producerRecord = new ProducerRecord<>(sendRequest.getTopic(), null, record.getKey(), record.getValue(), record.getHeaders());
                final Future<RecordMetadata> f = producer.send(producerRecord);
                try {
                    metadata.add(f.get());
                } catch (ExecutionException e) {
                    if (RuntimeException.class.isAssignableFrom(e.getCause().getClass())) throw (RuntimeException) e.getCause();
                    else throw new RuntimeException(e.getCause());
                }
            }
        } finally {
            producer.flush();
            producer.close();
        }
        return Collections.unmodifiableList(metadata);
    }

    @Override
    public <K, V> List<RecordMetadata> send(final SendKeyValuesTransactional<K, V> sendRequest) throws InterruptedException {
        final Producer<K, V> producer = new KafkaProducer<>(effectiveProducerProps(sendRequest.getProducerProps()));
        final List<RecordMetadata> metadata = new ArrayList<>();
        try {
            producer.initTransactions();
            producer.beginTransaction();
            for (String topic : sendRequest.getRecordsPerTopic().keySet()) {
                for (KeyValue<K, V> record : sendRequest.getRecordsPerTopic().get(topic)) {
                    final ProducerRecord<K, V> producerRecord = new ProducerRecord<K, V>(topic, null, record.getKey(), record.getValue(), record.getHeaders());
                    final Future<RecordMetadata> f = producer.send(producerRecord);
                    try {
                        metadata.add(f.get());
                    } catch (ExecutionException e) {
                        if (RuntimeException.class.isAssignableFrom(e.getCause().getClass())) throw (RuntimeException) e.getCause();
                        else throw new RuntimeException(e.getCause());
                    }
                }
            }
            if (sendRequest.shouldFailTransaction()) producer.abortTransaction();
            else producer.commitTransaction();
        } catch (ProducerFencedException e) {
            producer.abortTransaction();
            final String message = String.format(
                    "There happens to be another producer that shares the transactional ID '%s'" +
                            "with this producer, but that has a newer epoch assigned to it. This producer " +
                            "has been fenced off, it can no longer write to the log transactionally. Hence, " +
                            "the ongoing transaction is aborted and the producer closed.",
                    sendRequest.getProducerProps().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
            throw new RuntimeException(message, e);
        } catch (OutOfOrderSequenceException e) {
            producer.abortTransaction();
            final String message = "This producer has received out-of-band sequence numbers. This is a fatal condition " +
                    "and thus, the producer is no longer able to log transactionally and reliably. " +
                    "Hence, the ongoing transaction is aborted and the producer closed.";
            throw new RuntimeException(message, e);
        } finally {
            producer.flush();
            producer.close();
        }
        return Collections.unmodifiableList(metadata);
    }

    private Properties effectiveProducerProps(final Properties originalProducerProps) {
        final Properties effectiveProducerProps = new Properties();
        effectiveProducerProps.putAll(originalProducerProps);
        effectiveProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return effectiveProducerProps;
    }
}
