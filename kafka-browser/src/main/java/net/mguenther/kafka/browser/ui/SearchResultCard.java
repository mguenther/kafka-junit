package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.junit.KeyValue;

/**
 * An expandable card that displays a single search result record.
 * Shows key and value with expand/collapse triangles, plus offset and partition metadata.
 */
public class SearchResultCard extends VBox {

    private boolean keyExpanded = false;
    private boolean valueExpanded = false;

    public SearchResultCard(KeyValue<String, String> kv) {
        getStyleClass().add("search-result-card");
        setPadding(new Insets(10, 15, 10, 15));
        setSpacing(4);

        String key = kv.getKey() != null ? kv.getKey() : "";
        String value = kv.getValue() != null ? kv.getValue() : "";
        long offset = kv.getMetadata().map(m -> m.getOffset()).orElse(-1L);
        int partition = kv.getMetadata().map(m -> m.getPartition()).orElse(-1);

        // Key row
        HBox keyRow = new HBox(8);
        keyRow.setAlignment(Pos.CENTER_LEFT);

        Label keyTriangle = new Label("\u25B6");
        keyTriangle.getStyleClass().add("expand-triangle");

        Label keyLabel = new Label("key:");
        keyLabel.getStyleClass().add("result-field-label");

        Label keyValueLabel = new Label(truncate(key, 60));
        keyValueLabel.getStyleClass().add("result-field-value");
        HBox.setHgrow(keyValueLabel, Priority.ALWAYS);

        keyRow.getChildren().addAll(keyTriangle, keyLabel, keyValueLabel);

        // Key expanded content (hidden by default)
        Label keyFullContent = new Label(key);
        keyFullContent.getStyleClass().add("result-field-expanded");
        keyFullContent.setWrapText(true);
        keyFullContent.setVisible(false);
        keyFullContent.setManaged(false);

        keyRow.setOnMouseClicked(e -> {
            keyExpanded = !keyExpanded;
            keyTriangle.setText(keyExpanded ? "\u25BC" : "\u25B6");
            keyFullContent.setVisible(keyExpanded);
            keyFullContent.setManaged(keyExpanded);
        });

        // Value row
        HBox valueRow = new HBox(8);
        valueRow.setAlignment(Pos.CENTER_LEFT);

        Label valueTriangle = new Label("\u25B6");
        valueTriangle.getStyleClass().add("expand-triangle");

        Label valueLabel = new Label("value:");
        valueLabel.getStyleClass().add("result-field-label");

        Label valueValueLabel = new Label(truncate(value, 60));
        valueValueLabel.getStyleClass().add("result-field-value");
        HBox.setHgrow(valueValueLabel, Priority.ALWAYS);

        // Metadata on the right
        VBox metaBox = new VBox(2);
        metaBox.setAlignment(Pos.CENTER_RIGHT);
        Label offsetLabel = new Label("Offset: " + (offset >= 0 ? offset : "?"));
        offsetLabel.getStyleClass().add("result-meta-label");
        Label partitionLabel = new Label("Partition " + (partition >= 0 ? partition : "?"));
        partitionLabel.getStyleClass().add("result-meta-label");
        metaBox.getChildren().addAll(offsetLabel, partitionLabel);

        valueRow.getChildren().addAll(valueTriangle, valueLabel, valueValueLabel, metaBox);

        // Value expanded content
        Label valueFullContent = new Label(value);
        valueFullContent.getStyleClass().add("result-field-expanded");
        valueFullContent.setWrapText(true);
        valueFullContent.setVisible(false);
        valueFullContent.setManaged(false);

        valueRow.setOnMouseClicked(e -> {
            valueExpanded = !valueExpanded;
            valueTriangle.setText(valueExpanded ? "\u25BC" : "\u25B6");
            valueFullContent.setVisible(valueExpanded);
            valueFullContent.setManaged(valueExpanded);
        });

        getChildren().addAll(keyRow, keyFullContent, valueRow, valueFullContent);
    }

    private String truncate(String s, int maxLen) {
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen) + "\u2026";
    }
}
