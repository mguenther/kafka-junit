package net.mguenther.kafka.junit;

/**
 * Provides the means to manage Kafka topics. All of the operations a {@code TopicManager} provides
 * are synchronous in their nature.
 */
public interface TopicManager {

    /**
     * Creates the topic as defined by the given {@link TopicConfig} synchronously. This
     * method blocks as long as it takes to complete the underlying topic creation request.
     * Please note that this does not imply that the topic is usable directly after this
     * method returns due to an outstanding partition leader election.
     *
     * @param config
     *      provides the settings for the topic to create
     */
    void createTopic(TopicConfig config);

    /**
     * Marks the given {@code topic} for deletion. Please note that topics are not immediately
     * deleted from a Kafka cluster. This method will fail if the configuration of Kafka brokers
     * prohibits topic deletions and if the topic has already been marked for deletion.
     *
     * @param topic
     *      name of the topic that ought to be marked for deletion
     */
    void deleteTopic(String topic);

    /**
     * Determines whether the given {@code topic} exists.
     *
     * @param topic
     *      name of the topic that ought to be checked
     * @return
     *      {@code true} if the topic exists, {@code false} otherwise
     */
    boolean exists(String topic);
}
