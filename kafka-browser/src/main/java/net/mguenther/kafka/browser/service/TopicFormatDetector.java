package net.mguenther.kafka.browser.service;

import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.provider.DefaultRecordConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Detects the message format of a Kafka topic by sampling a few records
 * and inspecting their content.
 *
 * Detection heuristics:
 * - Avro: first byte is 0x00 (Confluent Schema Registry wire format magic byte)
 * - JSON: value starts with '{' or '[' and is valid-looking JSON
 * - Otherwise: raw/unknown ("...")
 */
public class TopicFormatDetector {

    private static final Logger LOG = LoggerFactory.getLogger(TopicFormatDetector.class);

    private final DefaultRecordConsumer consumer;

    public TopicFormatDetector(String bootstrapServers) {
        this.consumer = new DefaultRecordConsumer(bootstrapServers);
    }

    /**
     * Detects the format for the given topic by reading a small sample of records.
     *
     * @return "avro", "json", or null if unknown/empty
     */
    public String detect(String topic) {
        try {
            ReadKeyValues<String, String> request = ReadKeyValues.from(topic)
                    .withLimit(5)
                    .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                    .withMaxTotalPollTime(3, TimeUnit.SECONDS)
                    .build();

            List<KeyValue<String, String>> records = consumer.read(request);

            if (records.isEmpty()) {
                return null;
            }

            // Check the majority format
            int avroCount = 0;
            int jsonCount = 0;

            for (KeyValue<String, String> record : records) {
                String value = record.getValue();
                if (value == null || value.isEmpty()) continue;

                if (looksLikeAvro(value)) {
                    avroCount++;
                } else if (looksLikeJson(value)) {
                    jsonCount++;
                }
            }

            if (avroCount > jsonCount && avroCount > 0) {
                return "avro";
            } else if (jsonCount > 0) {
                return "json";
            }

            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted during format detection for topic {}", topic);
            return null;
        } catch (Exception e) {
            LOG.warn("Failed to detect format for topic {}: {}", topic, e.getMessage());
            return null;
        }
    }

    private boolean looksLikeAvro(String value) {
        // Confluent Avro wire format starts with magic byte 0x00 followed by 4-byte schema ID.
        // When deserialized as String, this typically produces a string starting with a null char.
        if (value.length() >= 5 && value.charAt(0) == '\0') {
            return true;
        }
        // Also check for binary-looking content (lots of non-printable chars)
        long nonPrintable = value.chars()
                .limit(20)
                .filter(c -> c < 32 && c != '\n' && c != '\r' && c != '\t')
                .count();
        return nonPrintable > 3;
    }

    private boolean looksLikeJson(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) return false;
        char first = trimmed.charAt(0);
        char last = trimmed.charAt(trimmed.length() - 1);
        return (first == '{' && last == '}') || (first == '[' && last == ']');
    }
}
