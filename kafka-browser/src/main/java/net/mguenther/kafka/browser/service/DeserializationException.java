package net.mguenther.kafka.browser.service;

/**
 * Thrown when records cannot be deserialized, typically due to a mismatch
 * between the selected deserializer and the actual record format.
 */
public class DeserializationException extends RuntimeException {

    public DeserializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
