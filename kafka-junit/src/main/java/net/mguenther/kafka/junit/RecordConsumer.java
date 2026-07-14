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
     * Reads values from a Kafka topic. This is a convenience method that accepts a
     * {@link net.mguenther.kafka.junit.ReadKeyValues.ReadKeyValuesBuilder} and
     * immediately constructs a {@link ReadKeyValues} request from it that is passed
     * on to {@link #readValues(ReadKeyValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the consumer and the read operation it
     *      has to carry out
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of consumed values
     * @see ReadKeyValues
     * @see ReadKeyValues.ReadKeyValuesBuilder
     */
    default <V> List<V> readValues(ReadKeyValues.ReadKeyValuesBuilder<String, V> builder) throws InterruptedException {
        return readValues(builder.build());
    }

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
     * Reads key-value pairs from a Kafka topic. This is a convenience method that accepts a
     * {@link net.mguenther.kafka.junit.ReadKeyValues.ReadKeyValuesBuilder} and immediately
     * constructs a {@link ReadKeyValues} request from it that is passed on to
     * {@link #read(ReadKeyValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the consumer and the read operation it
     *      has to carry out
     * @param <K>
     *      refers to the type of keys being read
     * @param <V>
     *      refers to the type of values being read
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of consumed key-value pairs
     * @see ReadKeyValues
     * @see ReadKeyValues.ReadKeyValuesBuilder
     */
    default <K, V> List<KeyValue<K, V>> read(ReadKeyValues.ReadKeyValuesBuilder<K, V> builder) throws InterruptedException {
        return read(builder.build());
    }

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the values that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed values does not meet the expected
     * number of records.
     *
     * @param observeRequest
     *      the configuration of the consumer and the observe operation it has to carry out
     * @param <V>
     *      refers to the type of values being read
     * @throws AssertionError
     *      in case the number of consumed values does not meet the expected number of records
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of values
     * @see ObserveKeyValues
     */
    <V> List<V> observeValues(ObserveKeyValues<String, V> observeRequest) throws InterruptedException;

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the values that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed values does not meet the expected
     * number of records. This is a convenience method that accepts a
     * {@link net.mguenther.kafka.junit.ObserveKeyValues.ObserveKeyValuesBuilder} and immediately
     * constructs a {@link ObserveKeyValues} request from it that is passed on to
     * {@link #observeValues(ObserveKeyValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the consumer and the observe operation it
     *      has to carry out
     * @param <V>
     *      refers to the type of values being read
     * @throws AssertionError
     *      in case the number of consumed values does not meet the expected number of records
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of values
     * @see ObserveKeyValues
     * @see ObserveKeyValues.ObserveKeyValuesBuilder
     */
    default <V> List<V> observeValues(ObserveKeyValues.ObserveKeyValuesBuilder<String, V> builder) throws InterruptedException {
        return observeValues(builder.build());
    }

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the key-value pairs that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed key-value pairs does not meet the expected
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

    /**
     * Observes a Kafka topic until a certain amount of records have been consumed or a timeout
     * elapses. Returns the key-value pairs that have been consumed up until this point or throws an
     * {@code AssertionError} if the number of consumed key-value pairs does not meet the expected
     * number of records. This is a convenience method that accepts a
     * {@link net.mguenther.kafka.junit.ObserveKeyValues.ObserveKeyValuesBuilder} and immediately
     * constructs a {@link ObserveKeyValues} request from it that is passed on to
     * {@link #observe(ObserveKeyValues)}.
     *
     * @param builder
     *      the builder used for the configuration of the consumer and the observe operation it
     *      has to carry out
     * @param <K>
     *      refers to the type of keys being read
     * @param <V>
     *      refers to the type of values being read
     * @throws AssertionError
     *      in case the number of consumed values does not meet the expected number of records
     * @throws InterruptedException
     *      in case an interrupt signal has been set
     * @return
     *      unmodifiable {@link java.util.List} of key-value pairs
     * @see ObserveKeyValues
     * @see ObserveKeyValues.ObserveKeyValuesBuilder
     */
    default <K, V> List<KeyValue<K, V>> observe(ObserveKeyValues.ObserveKeyValuesBuilder<K, V> builder) throws InterruptedException {
        return observe(builder.build());
    }
}
