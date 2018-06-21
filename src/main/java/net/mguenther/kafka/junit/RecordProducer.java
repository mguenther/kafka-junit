package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;

/**
 * Provides the means to send key-value pairs or un-keyed values to a Kafka topic. The send
 * operations a {@code RecordProducer} provides are synchronous in their nature.
 */
public interface RecordProducer {

    /**
     * Sends (un-keyed) values synchronously to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <V>
     *      refers to the type of values being send
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendValues
     */
    <V> List<RecordMetadata> send(SendValues<V> sendRequest) throws InterruptedException;

    /**
     * Sends (un-keyed) values synchronously and transactionally to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <V>
     *      refers to the type of values being send
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendValuesTransactional
     */
    <V> List<RecordMetadata> send(SendValuesTransactional<V> sendRequest) throws InterruptedException;

    /**
     * Sends key-value pairs synchronously to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <K>
     *      refers to the type of keys being send
     * @param <V>
     *      refers to the type of values being send
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValues
     */
    <K, V> List<RecordMetadata> send(SendKeyValues<K, V> sendRequest) throws InterruptedException;

    /**
     * Sends key-value pairs synchronously and transactionally to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <K>
     *      refers to the type of keys being send
     * @param <V>
     *      refers to the type of values being send
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValuesTransactional
     */
    <K, V> List<RecordMetadata> send(SendKeyValuesTransactional<K, V> sendRequest) throws InterruptedException;
}
