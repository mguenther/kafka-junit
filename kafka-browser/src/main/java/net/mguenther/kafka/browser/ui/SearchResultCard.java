package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/**
 * An expandable card that displays a single search result record.
 *
 * Shows:
 * - key: truncated preview (click arrow to expand full content)
 * - value: truncated preview (click arrow to expand, JSON is pretty-printed)
 * - headers: only shown if the record actually has headers
 * - offset + partition metadata on the right
 *
 * The preview is always a single line, truncated to a short length.
 * The expanded detail is shown in a constrained-height scrollable area
 * so large payloads don't blow up the card.
 */
public class SearchResultCard extends VBox {

    private static final int PREVIEW_MAX_CHARS = 80;
    private static final double EXPANDED_MAX_HEIGHT = 150;

    private boolean keyExpanded = false;
    private boolean valueExpanded = false;
    private boolean headersExpanded = false;

    public SearchResultCard(KeyValue<String, String> kv) {
        getStyleClass().add("search-result-card");
        setPadding(new Insets(10, 15, 10, 15));
        setSpacing(4);

        String key = kv.getKey() != null ? kv.getKey() : "";
        String value = kv.getValue() != null ? kv.getValue() : "";
        long offset = kv.getMetadata().map(m -> m.getOffset()).orElse(-1L);
        int partition = kv.getMetadata().map(m -> m.getPartition()).orElse(-1);
        Headers headers = kv.getHeaders();

        // Metadata row (top, right-aligned)
        HBox metaRow = new HBox(12);
        metaRow.setAlignment(Pos.CENTER_RIGHT);
        Label offsetLabel = new Label("Offset: " + (offset >= 0 ? offset : "?"));
        offsetLabel.getStyleClass().add("result-meta-label");
        Label partitionLabel = new Label("Partition " + (partition >= 0 ? partition : "?"));
        partitionLabel.getStyleClass().add("result-meta-label");
        metaRow.getChildren().addAll(offsetLabel, partitionLabel);

        // Key row
        HBox keyRow = createCollapsedRow("key:", preview(key));
        Label keyTriangle = (Label) keyRow.getChildren().get(0);

        // Key expanded content
        ScrollPane keyExpandedPane = createExpandedPane(key);

        keyRow.setOnMouseClicked(e -> {
            keyExpanded = !keyExpanded;
            keyTriangle.setText(keyExpanded ? "\u25BE" : "\u25B8");
            keyExpandedPane.setVisible(keyExpanded);
            keyExpandedPane.setManaged(keyExpanded);
        });

        // Value row
        HBox valueRow = createCollapsedRow("value:", preview(value));
        Label valueTriangle = (Label) valueRow.getChildren().get(0);

        // Value expanded content (pretty-print JSON if applicable)
        String expandedValue = formatForExpansion(value);
        ScrollPane valueExpandedPane = createExpandedPane(expandedValue);

        valueRow.setOnMouseClicked(e -> {
            valueExpanded = !valueExpanded;
            valueTriangle.setText(valueExpanded ? "\u25BE" : "\u25B8");
            valueExpandedPane.setVisible(valueExpanded);
            valueExpandedPane.setManaged(valueExpanded);
        });

        getChildren().addAll(metaRow, keyRow, keyExpandedPane, valueRow, valueExpandedPane);

        // Headers (only if present)
        if (headers != null && headers.toArray().length > 0) {
            HBox headersRow = createCollapsedRow("headers:", headersPreview(headers));
            Label headersTriangle = (Label) headersRow.getChildren().get(0);

            String headersDetail = formatHeaders(headers);
            ScrollPane headersExpandedPane = createExpandedPane(headersDetail);

            headersRow.setOnMouseClicked(e -> {
                headersExpanded = !headersExpanded;
                headersTriangle.setText(headersExpanded ? "\u25BE" : "\u25B8");
                headersExpandedPane.setVisible(headersExpanded);
                headersExpandedPane.setManaged(headersExpanded);
            });

            // Style headers row to look de-emphasized
            headersRow.getStyleClass().add("header-row-optional");

            getChildren().addAll(headersRow, headersExpandedPane);
        }
    }

    private HBox createCollapsedRow(String fieldName, String previewText) {
        HBox row = new HBox(8);
        row.setAlignment(Pos.CENTER_LEFT);
        row.setCursor(javafx.scene.Cursor.HAND);

        Label triangle = new Label("\u25B8");
        triangle.getStyleClass().add("expand-triangle");

        Label label = new Label(fieldName);
        label.getStyleClass().add("result-field-label");
        label.setMinWidth(Label.USE_PREF_SIZE);

        Label valueLabel = new Label(previewText);
        valueLabel.getStyleClass().add("result-field-value");
        valueLabel.setMaxWidth(Double.MAX_VALUE);
        HBox.setHgrow(valueLabel, Priority.ALWAYS);

        row.getChildren().addAll(triangle, label, valueLabel);
        return row;
    }

    private ScrollPane createExpandedPane(String fullContent) {
        Label contentLabel = new Label(fullContent);
        contentLabel.getStyleClass().add("result-field-expanded");
        contentLabel.setWrapText(true);
        contentLabel.setPadding(new Insets(4, 0, 4, 30));

        ScrollPane scrollPane = new ScrollPane(contentLabel);
        scrollPane.setFitToWidth(true);
        scrollPane.setMaxHeight(EXPANDED_MAX_HEIGHT);
        scrollPane.setPrefHeight(ScrollPane.USE_COMPUTED_SIZE);
        scrollPane.getStyleClass().add("expanded-scroll-pane");
        scrollPane.setVisible(false);
        scrollPane.setManaged(false);

        return scrollPane;
    }

    /**
     * Creates a single-line preview: collapses whitespace and truncates.
     */
    private String preview(String s) {
        if (s == null || s.isEmpty()) return "";
        // Collapse all whitespace (newlines, tabs, multiple spaces) into single spaces
        String collapsed = s.replaceAll("\\s+", " ").trim();
        if (collapsed.length() <= PREVIEW_MAX_CHARS) return collapsed;
        return collapsed.substring(0, PREVIEW_MAX_CHARS) + "\u2026";
    }

    /**
     * For expanded view: if it looks like JSON, attempt to pretty-print it.
     * Otherwise return as-is (but capped at a reasonable length).
     */
    private String formatForExpansion(String s) {
        if (s == null || s.isEmpty()) return "";
        String trimmed = s.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return prettyPrintJson(trimmed);
        }
        // For non-JSON, cap at a reasonable length for display
        if (s.length() > 2000) {
            return s.substring(0, 2000) + "\n\u2026 [truncated]";
        }
        return s;
    }

    /**
     * Simple JSON indentation without a library dependency.
     * Handles nested objects/arrays with 2-space indent.
     */
    private String prettyPrintJson(String json) {
        try {
            StringBuilder sb = new StringBuilder();
            int indent = 0;
            boolean inString = false;
            boolean escaped = false;

            for (int i = 0; i < json.length(); i++) {
                char c = json.charAt(i);

                if (escaped) {
                    sb.append(c);
                    escaped = false;
                    continue;
                }

                if (c == '\\' && inString) {
                    sb.append(c);
                    escaped = true;
                    continue;
                }

                if (c == '"') {
                    inString = !inString;
                    sb.append(c);
                    continue;
                }

                if (inString) {
                    sb.append(c);
                    continue;
                }

                switch (c) {
                    case '{', '[' -> {
                        sb.append(c);
                        sb.append('\n');
                        indent += 2;
                        sb.append(" ".repeat(indent));
                    }
                    case '}', ']' -> {
                        sb.append('\n');
                        indent -= 2;
                        sb.append(" ".repeat(Math.max(0, indent)));
                        sb.append(c);
                    }
                    case ',' -> {
                        sb.append(c);
                        sb.append('\n');
                        sb.append(" ".repeat(indent));
                    }
                    case ':' -> sb.append(": ");
                    case ' ', '\t', '\n', '\r' -> {} // skip whitespace
                    default -> sb.append(c);
                }
            }
            return sb.toString();
        } catch (Exception e) {
            // If pretty-printing fails, return original
            return json;
        }
    }

    private String headersPreview(Headers headers) {
        int count = 0;
        StringBuilder sb = new StringBuilder();
        for (Header h : headers) {
            if (count > 0) sb.append(", ");
            sb.append(h.key());
            count++;
            if (count >= 3) {
                int remaining = headers.toArray().length - count;
                if (remaining > 0) sb.append(" (+").append(remaining).append(" more)");
                break;
            }
        }
        return sb.toString();
    }

    private String formatHeaders(Headers headers) {
        StringBuilder sb = new StringBuilder();
        for (Header h : headers) {
            sb.append(h.key()).append(": ");
            if (h.value() != null) {
                sb.append(new String(h.value(), StandardCharsets.UTF_8));
            } else {
                sb.append("<null>");
            }
            sb.append("\n");
        }
        return sb.toString().trim();
    }
}
