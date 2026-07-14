package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An Environment represents a named set of configuration parameters
 * (key-value pairs) that can be applied when connecting to a Kafka cluster.
 * Examples: "Localhost", "Integration", "Production".
 */
public class Environment {

    private String name;
    private Map<String, String> parameters;

    @JsonCreator
    public Environment(@JsonProperty("name") String name,
                       @JsonProperty("parameters") Map<String, String> parameters) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.parameters = parameters != null ? new LinkedHashMap<>(parameters) : new LinkedHashMap<>();
    }

    public Environment(String name) {
        this(name, new LinkedHashMap<>());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = Objects.requireNonNull(name);
    }

    public Map<String, String> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = new LinkedHashMap<>(parameters);
    }

    public void putParameter(String key, String value) {
        this.parameters.put(key, value);
    }

    public void removeParameter(String key) {
        this.parameters.remove(key);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Environment that = (Environment) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
