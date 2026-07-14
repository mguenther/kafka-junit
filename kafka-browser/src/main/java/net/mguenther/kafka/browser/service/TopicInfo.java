package net.mguenther.kafka.browser.service;

/**
 * Holds metadata about a Kafka topic: its name, number of partitions and replicas.
 */
public class TopicInfo {

    private final String name;
    private final int partitions;
    private final int replicas;
    private String format; // "avro", "json", or null (unknown)

    public TopicInfo(String name, int partitions, int replicas) {
        this.name = name;
        this.partitions = partitions;
        this.replicas = replicas;
        this.format = null;
    }

    public String getName() {
        return name;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getReplicas() {
        return replicas;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return name + " (" + partitions + " partitions, " + replicas + " replicas)";
    }
}
