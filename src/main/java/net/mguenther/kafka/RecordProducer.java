package net.mguenther.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Provides the means to send key-value pairs or un-keyed values to a Kafka topic. The send
 * operations a {@code RecordProducer} provides are synchronous in their nature.
 */
public interface RecordProducer {

    /**
     * Sends (un-keyed) values <emph>synchronously</emph> to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @throws ExecutionException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     */
    <V> List<RecordMetadata> send(SendValues<V> sendRequest) throws ExecutionException, InterruptedException;

    /**
     * Sends key-value pairs <emph>synchronously</emph> to a Kafka topic.
     *
     * @param sendRequest
     *      the configuration of the producer and the send operation it has to carry out
     * @throws ExecutionException
     *      in case there is an error while sending an individual Kafka record to the
     *      designated Kafka broker
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} that contains metadata on the written
     *      Kafka records
     * @see SendKeyValues
     */
    <K, V> List<RecordMetadata> send(SendKeyValues<K, V> sendRequest) throws ExecutionException, InterruptedException;
}
