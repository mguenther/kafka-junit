package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * An Environment represents a named connection target for a Kafka cluster.
 * Each environment defines where and how to connect:
 * - Bootstrap servers
 * - ZooKeeper URL (optional, for older Kafka versions)
 * - Security protocol and settings
 * - Additional Kafka client properties
 *
 * Examples: "Local", "Test", "Production"
 */
public class Environment {

    private String name;
    private String bootstrapServers;
    private String zookeeperConnectUrl;
    private String kafkaVersion;

    // Security settings
    private String securityProtocol; // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    private String saslMechanism;    // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
    private String saslJaasConfig;   // Free-text JAAS config
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;

    // Additional properties (key-value overrides)
    private Map<String, String> additionalProperties;

    @JsonCreator
    public Environment(@JsonProperty("name") String name,
                       @JsonProperty("bootstrapServers") String bootstrapServers,
                       @JsonProperty("zookeeperConnectUrl") String zookeeperConnectUrl,
                       @JsonProperty("kafkaVersion") String kafkaVersion,
                       @JsonProperty("securityProtocol") String securityProtocol,
                       @JsonProperty("saslMechanism") String saslMechanism,
                       @JsonProperty("saslJaasConfig") String saslJaasConfig,
                       @JsonProperty("sslTruststoreLocation") String sslTruststoreLocation,
                       @JsonProperty("sslTruststorePassword") String sslTruststorePassword,
                       @JsonProperty("sslKeystoreLocation") String sslKeystoreLocation,
                       @JsonProperty("sslKeystorePassword") String sslKeystorePassword,
                       @JsonProperty("additionalProperties") Map<String, String> additionalProperties) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.bootstrapServers = bootstrapServers != null ? bootstrapServers : "";
        this.zookeeperConnectUrl = zookeeperConnectUrl != null ? zookeeperConnectUrl : "";
        this.kafkaVersion = kafkaVersion != null ? kafkaVersion : "3.x (KRaft)";
        this.securityProtocol = securityProtocol != null ? securityProtocol : "PLAINTEXT";
        this.saslMechanism = saslMechanism;
        this.saslJaasConfig = saslJaasConfig;
        this.sslTruststoreLocation = sslTruststoreLocation;
        this.sslTruststorePassword = sslTruststorePassword;
        this.sslKeystoreLocation = sslKeystoreLocation;
        this.sslKeystorePassword = sslKeystorePassword;
        this.additionalProperties = additionalProperties != null ? new LinkedHashMap<>(additionalProperties) : new LinkedHashMap<>();
    }

    public Environment(String name, String bootstrapServers) {
        this(name, bootstrapServers, "", "3.x (KRaft)", "PLAINTEXT",
                null, null, null, null, null, null, new LinkedHashMap<>());
    }

    public Environment(String name) {
        this(name, "");
    }

    // --- Getters and setters ---

    public String getName() { return name; }
    public void setName(String name) { this.name = Objects.requireNonNull(name); }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }

    public String getZookeeperConnectUrl() { return zookeeperConnectUrl; }
    public void setZookeeperConnectUrl(String zookeeperConnectUrl) { this.zookeeperConnectUrl = zookeeperConnectUrl; }

    public String getKafkaVersion() { return kafkaVersion; }
    public void setKafkaVersion(String kafkaVersion) { this.kafkaVersion = kafkaVersion; }

    public String getSecurityProtocol() { return securityProtocol; }
    public void setSecurityProtocol(String securityProtocol) { this.securityProtocol = securityProtocol; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String saslMechanism) { this.saslMechanism = saslMechanism; }

    public String getSaslJaasConfig() { return saslJaasConfig; }
    public void setSaslJaasConfig(String saslJaasConfig) { this.saslJaasConfig = saslJaasConfig; }

    public String getSslTruststoreLocation() { return sslTruststoreLocation; }
    public void setSslTruststoreLocation(String sslTruststoreLocation) { this.sslTruststoreLocation = sslTruststoreLocation; }

    public String getSslTruststorePassword() { return sslTruststorePassword; }
    public void setSslTruststorePassword(String sslTruststorePassword) { this.sslTruststorePassword = sslTruststorePassword; }

    public String getSslKeystoreLocation() { return sslKeystoreLocation; }
    public void setSslKeystoreLocation(String sslKeystoreLocation) { this.sslKeystoreLocation = sslKeystoreLocation; }

    public String getSslKeystorePassword() { return sslKeystorePassword; }
    public void setSslKeystorePassword(String sslKeystorePassword) { this.sslKeystorePassword = sslKeystorePassword; }

    public Map<String, String> getAdditionalProperties() { return additionalProperties; }
    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = new LinkedHashMap<>(additionalProperties);
    }

    /**
     * Builds a Properties object with all Kafka client connection settings
     * derived from this environment configuration.
     */
    public Properties toKafkaProperties() {
        Properties props = new Properties();
        if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
            props.put("bootstrap.servers", bootstrapServers);
        }
        if (securityProtocol != null && !"PLAINTEXT".equals(securityProtocol)) {
            props.put("security.protocol", securityProtocol);
        }
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            props.put("sasl.mechanism", saslMechanism);
        }
        if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
            props.put("sasl.jaas.config", saslJaasConfig);
        }
        if (sslTruststoreLocation != null && !sslTruststoreLocation.isEmpty()) {
            props.put("ssl.truststore.location", sslTruststoreLocation);
            if (sslTruststorePassword != null && !sslTruststorePassword.isEmpty()) {
                props.put("ssl.truststore.password", sslTruststorePassword);
            }
        }
        if (sslKeystoreLocation != null && !sslKeystoreLocation.isEmpty()) {
            props.put("ssl.keystore.location", sslKeystoreLocation);
            if (sslKeystorePassword != null && !sslKeystorePassword.isEmpty()) {
                props.put("ssl.keystore.password", sslKeystorePassword);
            }
        }
        if (additionalProperties != null) {
            additionalProperties.forEach(props::put);
        }
        return props;
    }

    @Override
    public String toString() { return name; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Environment that = (Environment) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() { return Objects.hash(name); }
}
