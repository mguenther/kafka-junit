package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A Workspace represents a named Kafka cluster connection configuration.
 * It holds the bootstrap servers, optionally ZooKeeper connection URL,
 * and a list of environments that can be activated to override/extend
 * the base configuration.
 */
public class Workspace {

    private String name;
    private String bootstrapServers;
    private String zookeeperConnectUrl;
    private String kafkaVersion;
    private List<Environment> environments;

    @JsonCreator
    public Workspace(@JsonProperty("name") String name,
                     @JsonProperty("bootstrapServers") String bootstrapServers,
                     @JsonProperty("zookeeperConnectUrl") String zookeeperConnectUrl,
                     @JsonProperty("kafkaVersion") String kafkaVersion,
                     @JsonProperty("environments") List<Environment> environments) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.bootstrapServers = bootstrapServers != null ? bootstrapServers : "";
        this.zookeeperConnectUrl = zookeeperConnectUrl != null ? zookeeperConnectUrl : "";
        this.kafkaVersion = kafkaVersion != null ? kafkaVersion : "3.x (KRaft)";
        this.environments = environments != null ? new ArrayList<>(environments) : new ArrayList<>();
    }

    public Workspace(String name, String bootstrapServers, String zookeeperConnectUrl, String kafkaVersion) {
        this(name, bootstrapServers, zookeeperConnectUrl, kafkaVersion, new ArrayList<>());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = Objects.requireNonNull(name);
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getZookeeperConnectUrl() {
        return zookeeperConnectUrl;
    }

    public void setZookeeperConnectUrl(String zookeeperConnectUrl) {
        this.zookeeperConnectUrl = zookeeperConnectUrl;
    }

    public String getKafkaVersion() {
        return kafkaVersion;
    }

    public void setKafkaVersion(String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
    }

    public List<Environment> getEnvironments() {
        return Collections.unmodifiableList(environments);
    }

    public void setEnvironments(List<Environment> environments) {
        this.environments = new ArrayList<>(environments);
    }

    public void addEnvironment(Environment env) {
        this.environments.add(env);
    }

    public void removeEnvironment(Environment env) {
        this.environments.remove(env);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Workspace workspace = (Workspace) o;
        return Objects.equals(name, workspace.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
