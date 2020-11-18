package net.mguenther.kafka.junit;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;

/**
 * Provides the means to send key-value pairs or un-keyed values to a Kafka topic. The send
 * operations a {@code RecordProducer} provides are synchronous in their nature.
 */
public interface RecordProducer {

    /**
     * Sends (non-keyed) values synchronously to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <V>
     *      refers to the type of values being sent
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
     * Sends (non-keyed) values synchronously to a Kafka topic. This is a convenience
     * method that accepts a {@link net.mguenther.kafka.junit.SendValues.SendValuesBuilder}
     * and immediately constructs a {@link SendValues} request from it that is passed
     * on to {@link #send(SendValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the producer and the send operation
     *      it has to carry out
     * @param <V>
     *      refers to the type of values being sent
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendValues
     * @see SendValues.SendValuesBuilder
     */
    default <V> List<RecordMetadata> send(SendValues.SendValuesBuilder<V> builder) throws InterruptedException {
        return send(builder.build());
    }

    /**
     * Sends (non-keyed) values synchronously and within a transaction to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <V>
     *      refers to the type of values being sent
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
     * Sends (non-keyed) values synchronously and within a transaction to a Kafka topic.
     * This is a convenience method that accepts a {@link net.mguenther.kafka.junit.SendValuesTransactional.SendValuesTransactionalBuilder}
     * and immediately constructs a {@link SendKeyValuesTransactional} request from it that
     * is passed on to {@link #send(SendValuesTransactional)}.
     *
     * @param builder
     *      the builder used for the configuration of the producer and the send operation
     *      it has to carry out
     * @param <V>
     *      refers to the type of values being sent
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendValuesTransactional
     * @see SendValuesTransactional.SendValuesTransactionalBuilder
     */
    default <V> List<RecordMetadata> send(SendValuesTransactional.SendValuesTransactionalBuilder<V> builder) throws InterruptedException {
        return send(builder.build());
    }

    /**
     * Sends key-value pairs synchronously to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <K>
     *      refers to the type of keys being sent
     * @param <V>
     *      refers to the type of values being sent
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
     * Sends key-value pairs synchronously to a Kafka topic. This is a convenience method
     * that accepts a {@link net.mguenther.kafka.junit.SendKeyValues.SendKeyValuesBuilder}
     * and immediately constructs a {@link SendKeyValues} and is passed on to
     * {@link #send(SendKeyValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the producer and the send operation
     *      it has to carry out
     * @param <K>
     *      refers to the type of keys being sent
     * @param <V>
     *      refers to the type of values being sent
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValues
     * @see SendKeyValues.SendKeyValuesBuilder
     */
    default <K, V> List<RecordMetadata> send(SendKeyValues.SendKeyValuesBuilder<K, V> builder) throws InterruptedException {
        return send(builder.build());
    }

    /**
     * Sends key-value pairs synchronously and within a transaction to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @param <K>
     *      refers to the type of keys being sent
     * @param <V>
     *      refers to the type of values being sent
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

    /**
     * Sends key-value pairs synchronously and within a transaction to a Kafka topic.
     * This is a convenience method that accepts a
     * {@link net.mguenther.kafka.junit.SendKeyValuesTransactional.SendKeyValuesTransactionalBuilder}
     * and immediately constructs a {@link SendKeyValuesTransactional} request from it that is
     * passed on to {@link #send(SendKeyValuesTransactional)}.
     *
     * @param builder
     *      the builder used for the configuration of the producer and the send operation
     *      it has to carry out
     * @param <K>
     *      refers to the type of keys being sent
     * @param <V>
     *      refers to the type of values being sent
     * @throws RuntimeException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValuesTransactional
     * @see SendKeyValuesTransactional.SendKeyValuesTransactionalBuilder
     */
    default <K, V> List<RecordMetadata> send(SendKeyValuesTransactional.SendKeyValuesTransactionalBuilder<K, V> builder) throws InterruptedException {
        return send(builder.build());
    }
}
