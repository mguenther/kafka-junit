package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Handles reading and writing of BrowserConfig to a JSON file in the
 * user's home directory (~/.kafka-browser/config.json).
 */
public class ConfigPersistence {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigPersistence.class);
    private static final String CONFIG_DIR = ".kafka-browser";
    private static final String CONFIG_FILE = "config.json";

    private final ObjectMapper objectMapper;
    private final Path configPath;

    public ConfigPersistence() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path homeDir = Paths.get(System.getProperty("user.home"));
        this.configPath = homeDir.resolve(CONFIG_DIR).resolve(CONFIG_FILE);
    }

    public BrowserConfig load() {
        if (!Files.exists(configPath)) {
            LOG.info("No configuration file found at {}. Starting with empty config.", configPath);
            return new BrowserConfig();
        }
        try {
            return objectMapper.readValue(configPath.toFile(), BrowserConfig.class);
        } catch (IOException e) {
            LOG.error("Failed to load configuration from {}. Starting with empty config.", configPath, e);
            return new BrowserConfig();
        }
    }

    public void save(BrowserConfig config) {
        try {
            Path dir = configPath.getParent();
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            objectMapper.writeValue(configPath.toFile(), config);
            LOG.info("Configuration saved to {}.", configPath);
        } catch (IOException e) {
            LOG.error("Failed to save configuration to {}.", configPath, e);
        }
    }

    public Path getConfigPath() {
        return configPath;
    }
}
