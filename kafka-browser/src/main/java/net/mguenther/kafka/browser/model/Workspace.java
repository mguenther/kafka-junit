package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A Workspace represents a project-oriented view of a Kafka cluster.
 * It defines:
 * - A name (e.g., "Order Service")
 * - Topic filter patterns (comma-separated globs, e.g., "order-*,payment-*")
 * - A list of environments (connection targets like Local, Test, Production)
 *
 * The workspace does NOT hold connection settings directly — those live in
 * each Environment.
 */
public class Workspace {

    private String name;
    private String topicFilter;
    private List<Environment> environments;

    @JsonCreator
    public Workspace(@JsonProperty("name") String name,
                     @JsonProperty("topicFilter") String topicFilter,
                     @JsonProperty("environments") List<Environment> environments,
                     // Legacy fields — ignored on read but kept for backward compat
                     @JsonProperty("bootstrapServers") String bootstrapServers,
                     @JsonProperty("zookeeperConnectUrl") String zookeeperConnectUrl,
                     @JsonProperty("kafkaVersion") String kafkaVersion) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.topicFilter = topicFilter != null ? topicFilter : "";
        this.environments = environments != null ? new ArrayList<>(environments) : new ArrayList<>();

        // Migration: if old-style workspace had bootstrapServers but no environments,
        // create a default "Local" environment from the legacy fields
        if (this.environments.isEmpty() && bootstrapServers != null && !bootstrapServers.isEmpty()) {
            Environment migrated = new Environment("Local", bootstrapServers);
            migrated.setZookeeperConnectUrl(zookeeperConnectUrl != null ? zookeeperConnectUrl : "");
            migrated.setKafkaVersion(kafkaVersion != null ? kafkaVersion : "3.x (KRaft)");
            this.environments.add(migrated);
        }
    }

    public Workspace(String name, String topicFilter) {
        this(name, topicFilter, new ArrayList<>(), null, null, null);
    }

    public Workspace(String name) {
        this(name, "");
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = Objects.requireNonNull(name); }

    public String getTopicFilter() { return topicFilter; }
    public void setTopicFilter(String topicFilter) { this.topicFilter = topicFilter != null ? topicFilter : ""; }

    public List<Environment> getEnvironments() { return Collections.unmodifiableList(environments); }
    public void setEnvironments(List<Environment> environments) { this.environments = new ArrayList<>(environments); }
    public void addEnvironment(Environment env) { this.environments.add(env); }
    public void removeEnvironment(Environment env) { this.environments.remove(env); }

    /**
     * Tests whether a topic name matches the configured filter patterns.
     * Filter is comma-separated glob patterns (e.g., "order-*,payment-*").
     * An empty filter matches all topics.
     */
    public boolean matchesTopic(String topicName) {
        if (topicFilter == null || topicFilter.isBlank()) {
            return true; // no filter = show all
        }
        String[] patterns = topicFilter.split(",");
        for (String pattern : patterns) {
            String trimmed = pattern.trim();
            if (trimmed.isEmpty()) continue;
            if (globMatches(trimmed, topicName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Simple glob matching: supports * (any chars) and ? (single char).
     */
    private boolean globMatches(String pattern, String text) {
        String regex = "^" + pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".")
                + "$";
        return text.matches(regex);
    }

    @Override
    public String toString() { return name; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Workspace workspace = (Workspace) o;
        return Objects.equals(name, workspace.name);
    }

    @Override
    public int hashCode() { return Objects.hash(name); }
}
