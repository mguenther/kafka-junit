package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.junit.KeyValue;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/**
 * In-app overlay dialog that shows full details of a single Kafka record:
 * - Topic, Partition, Offset
 * - Headers (only if present)
 * - Key (full content)
 * - Value (pretty-printed if JSON)
 * - Close button
 */
public class RecordDetailDialog extends OverlayDialog<Void> {

    public RecordDetailDialog(KeyValue<String, String> kv, String topicName) {
        super();

        dialogPane.setMaxWidth(700);

        String key = kv.getKey() != null ? kv.getKey() : "";
        String value = kv.getValue() != null ? kv.getValue() : "";
        long offset = kv.getMetadata().map(m -> m.getOffset()).orElse(-1L);
        int partition = kv.getMetadata().map(m -> m.getPartition()).orElse(-1);
        Headers headers = kv.getHeaders();

        // Title
        Label title = new Label("Record Detail");
        title.getStyleClass().add("overlay-dialog-title");

        // Metadata row
        HBox metaRow = new HBox(20);
        metaRow.setAlignment(Pos.CENTER_LEFT);
        metaRow.setPadding(new Insets(5, 0, 10, 0));

        Label topicLabel = new Label("Topic: " + topicName);
        topicLabel.getStyleClass().add("record-detail-meta");

        Label partitionLabel = new Label("Partition: " + (partition >= 0 ? partition : "?"));
        partitionLabel.getStyleClass().add("record-detail-meta");

        Label offsetLabel = new Label("Offset: " + (offset >= 0 ? offset : "?"));
        offsetLabel.getStyleClass().add("record-detail-meta");

        metaRow.getChildren().addAll(topicLabel, partitionLabel, offsetLabel);

        dialogPane.setSpacing(8);
        dialogPane.getChildren().addAll(title, metaRow);

        // Headers (only if present)
        if (headers != null && headers.toArray().length > 0) {
            Label headersLabel = new Label("Headers");
            headersLabel.getStyleClass().add("dialog-field-label-optional");

            VBox headersContent = new VBox(2);
            headersContent.setPadding(new Insets(4, 0, 4, 10));
            for (Header h : headers) {
                String headerValue = h.value() != null
                        ? new String(h.value(), StandardCharsets.UTF_8)
                        : "<null>";
                Label headerLine = new Label(h.key() + ": " + headerValue);
                headerLine.getStyleClass().add("record-detail-header-line");
                headersContent.getChildren().add(headerLine);
            }

            dialogPane.getChildren().addAll(headersLabel, headersContent);
        }

        // Key
        Label keyLabel = new Label("Key");
        keyLabel.getStyleClass().add("dialog-field-label");

        TextArea keyArea = new TextArea(key);
        keyArea.getStyleClass().add("record-detail-text-area");
        keyArea.setEditable(false);
        keyArea.setWrapText(true);
        keyArea.setPrefRowCount(Math.min(3, countLines(key)));

        dialogPane.getChildren().addAll(keyLabel, keyArea);

        // Value
        Label valueLabel = new Label("Value");
        valueLabel.getStyleClass().add("dialog-field-label");

        String formattedValue = formatValue(value);
        TextArea valueArea = new TextArea(formattedValue);
        valueArea.getStyleClass().add("record-detail-text-area");
        valueArea.setEditable(false);
        valueArea.setWrapText(true);
        valueArea.setPrefRowCount(Math.min(15, Math.max(5, countLines(formattedValue))));
        VBox.setVgrow(valueArea, Priority.ALWAYS);

        dialogPane.getChildren().addAll(valueLabel, valueArea);

        // Close button
        HBox buttonRow = new HBox();
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(10, 0, 0, 0));

        Button closeBtn = new Button("Close");
        closeBtn.getStyleClass().add("dialog-cancel-button");
        closeBtn.setOnAction(e -> close(null));

        buttonRow.getChildren().add(closeBtn);
        dialogPane.getChildren().add(buttonRow);
    }

    private String formatValue(String value) {
        if (value == null || value.isEmpty()) return "";
        String trimmed = value.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}"))
                || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return prettyPrintJson(trimmed);
        }
        return value;
    }

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
                    case ' ', '\t', '\n', '\r' -> {}
                    default -> sb.append(c);
                }
            }
            return sb.toString();
        } catch (Exception e) {
            return json;
        }
    }

    private int countLines(String text) {
        if (text == null || text.isEmpty()) return 1;
        return (int) text.lines().count();
    }
}
