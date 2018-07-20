package net.mguenther.kafka.junit;

import java.util.List;

/**
 * Provides the means to read key-value pairs or un-keyed values from a Kafka topic as well
 * as the possibility to watch given topics until a certain amount of records have been consumed
 * from them. All of the operations a {@code RecordConsumer} provides are synchronous in their
 * nature.
 */
public interface RecordConsumer {

    /**
     * Reads values from a Kafka topic.
     *
     * @param readRequest
     *      the configuration of the consumer and the read operation it has to carry out
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of consumed values
     * @see ReadKeyValues
     */
    <V> List<V> readValues(ReadKeyValues<String, V> readRequest) throws InterruptedException;

    /**
     * Reads key-value pairs from a Kafka topic.
     *
     * @param readRequest
     *      the configuration of the consumer and the read operation it has to carry out
     * @param <K>
     *      refers to the type of keys being read
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of consumed key-value pairs
     * @see ReadKeyValues
     */
    <K, V> List<KeyValue<K, V>> read(ReadKeyValues<K, V> readRequest) throws InterruptedException;

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the values that have been consumed up until this point of throws an
     * {@code AssertionError} if the number of consumed values does not meet the expected
     * number of records.
     *
     * @param observeRequest
     *      the configuration of the consumer and the observe operation it has to carry out
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of values
     * @see ObserveKeyValues
     */
    <V> List<V> observeValues(ObserveKeyValues<String, V> observeRequest) throws InterruptedException;

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the key-value-pairs that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed key-value-pairs does not meet the expected
     * number of records.
     *
     * @param observeRequest
     *      the configuration of the consumer and the observe operation it has to carry out
     * @param <K>
     *      refers to the type of keys being read
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of key-value pairs
     * @see ObserveKeyValues
     */
    <K, V> List<KeyValue<K, V>> observe(ObserveKeyValues<K, V> observeRequest) throws InterruptedException;
}
