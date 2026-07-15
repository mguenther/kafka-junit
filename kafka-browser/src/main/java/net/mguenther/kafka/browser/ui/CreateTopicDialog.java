package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.junit.TopicConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * In-app overlay dialog for creating a new Kafka topic.
 * Fields:
 * - Topic Name
 * - Number of Partitions
 * - Replication Factor
 * - Cleanup Policy (compact / delete)
 * - Min In-Sync Replicas
 * - Retention (ms)
 * - Additional properties (dynamic key-value rows)
 */
public class CreateTopicDialog extends OverlayDialog<TopicConfig> {

    private TextField nameField;
    private Spinner<Integer> partitionsSpinner;
    private Spinner<Integer> replicationSpinner;
    private Spinner<Integer> minIsrSpinner;
    private TextField cleanupPolicyField;
    private TextField retentionField;
    private VBox additionalPropsContainer;
    private final List<PropertyRow> propertyRows = new ArrayList<>();

    public CreateTopicDialog() {
        super();

        dialogPane.setMaxWidth(550);

        // Title
        Label title = new Label("Create Topic");
        title.getStyleClass().add("overlay-dialog-title");

        // Topic Name
        Label nameLabel = new Label("Topic Name");
        nameLabel.getStyleClass().add("dialog-field-label");
        nameField = new TextField();
        nameField.setPromptText("my-topic");
        nameField.getStyleClass().add("dialog-text-field");

        // Partitions
        Label partitionsLabel = new Label("Number of Partitions");
        partitionsLabel.getStyleClass().add("dialog-field-label");
        partitionsSpinner = new Spinner<>();
        partitionsSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 256, 1));
        partitionsSpinner.setEditable(true);
        partitionsSpinner.setPrefWidth(100);
        partitionsSpinner.getStyleClass().add("dialog-spinner");

        HBox partitionsRow = new HBox(15);
        partitionsRow.setAlignment(Pos.CENTER_LEFT);
        partitionsRow.getChildren().addAll(partitionsLabel, partitionsSpinner);

        // Replication Factor
        Label replicationLabel = new Label("Replication Factor");
        replicationLabel.getStyleClass().add("dialog-field-label");
        replicationSpinner = new Spinner<>();
        replicationSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 10, 1));
        replicationSpinner.setEditable(true);
        replicationSpinner.setPrefWidth(100);
        replicationSpinner.getStyleClass().add("dialog-spinner");

        HBox replicationRow = new HBox(15);
        replicationRow.setAlignment(Pos.CENTER_LEFT);
        replicationRow.getChildren().addAll(replicationLabel, replicationSpinner);

        // Cleanup Policy
        Label cleanupLabel = new Label("Cleanup Policy");
        cleanupLabel.getStyleClass().add("dialog-field-label");
        cleanupPolicyField = new TextField("delete");
        cleanupPolicyField.setPromptText("delete / compact");
        cleanupPolicyField.getStyleClass().add("dialog-text-field");
        cleanupPolicyField.setPrefWidth(200);

        HBox cleanupRow = new HBox(15);
        cleanupRow.setAlignment(Pos.CENTER_LEFT);
        cleanupRow.getChildren().addAll(cleanupLabel, cleanupPolicyField);

        // Min In-Sync Replicas
        Label minIsrLabel = new Label("Min In-Sync Replicas");
        minIsrLabel.getStyleClass().add("dialog-field-label");
        minIsrSpinner = new Spinner<>();
        minIsrSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 10, 1));
        minIsrSpinner.setEditable(true);
        minIsrSpinner.setPrefWidth(100);
        minIsrSpinner.getStyleClass().add("dialog-spinner");

        HBox minIsrRow = new HBox(15);
        minIsrRow.setAlignment(Pos.CENTER_LEFT);
        minIsrRow.getChildren().addAll(minIsrLabel, minIsrSpinner);

        // Retention (ms)
        Label retentionLabel = new Label("Retention (ms)");
        retentionLabel.getStyleClass().add("dialog-field-label");
        retentionField = new TextField("86400000");
        retentionField.setPromptText("86400000");
        retentionField.getStyleClass().add("dialog-text-field");
        retentionField.setPrefWidth(150);

        HBox retentionRow = new HBox(15);
        retentionRow.setAlignment(Pos.CENTER_LEFT);
        retentionRow.getChildren().addAll(retentionLabel, retentionField);

        // Additional properties section
        Label additionalLabel = new Label("Additional Properties");
        additionalLabel.getStyleClass().add("dialog-field-label");
        additionalLabel.setPadding(new Insets(8, 0, 0, 0));

        additionalPropsContainer = new VBox(6);
        addEmptyPropertyRow();

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button createBtn = new Button("Create");
        createBtn.getStyleClass().add("dialog-save-button");
        createBtn.setOnAction(e -> {
            String topicName = nameField.getText().trim();
            if (topicName.isEmpty()) {
                nameField.requestFocus();
                return;
            }
            TopicConfig.TopicConfigBuilder builder = TopicConfig.withName(topicName)
                    .withNumberOfPartitions(partitionsSpinner.getValue())
                    .withNumberOfReplicas(replicationSpinner.getValue())
                    .with("cleanup.policy", cleanupPolicyField.getText().trim())
                    .with("min.insync.replicas", String.valueOf(minIsrSpinner.getValue()))
                    .with("delete.retention.ms", retentionField.getText().trim());

            // Add custom properties
            for (PropertyRow row : propertyRows) {
                String key = row.keyField.getText().trim();
                String value = row.valueField.getText().trim();
                if (!key.isEmpty()) {
                    builder.with(key, value);
                }
            }

            close(builder.build());
        });

        buttonRow.getChildren().addAll(cancelBtn, createBtn);

        dialogPane.setSpacing(8);
        dialogPane.getChildren().addAll(
                title,
                nameLabel, nameField,
                partitionsRow,
                replicationRow,
                cleanupRow,
                minIsrRow,
                retentionRow,
                additionalLabel, additionalPropsContainer,
                buttonRow
        );
    }

    private void addEmptyPropertyRow() {
        PropertyRow row = new PropertyRow("", "");
        additionalPropsContainer.getChildren().add(row.container);

        row.keyField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.isEmpty() && !propertyRows.contains(row)) {
                propertyRows.add(row);
                row.removeBtn.setVisible(true);
                row.removeBtn.setManaged(true);
                addEmptyPropertyRow();
            }
        });
    }

    private void removePropertyRow(PropertyRow row) {
        propertyRows.remove(row);
        additionalPropsContainer.getChildren().remove(row.container);
    }

    private class PropertyRow {
        HBox container;
        TextField keyField;
        TextField valueField;
        Button removeBtn;

        PropertyRow(String key, String value) {
            container = new HBox(10);
            container.setAlignment(Pos.CENTER_LEFT);

            keyField = new TextField(key);
            keyField.setPromptText("<property>");
            keyField.getStyleClass().add("dialog-text-field");
            keyField.setPrefWidth(200);

            valueField = new TextField(value);
            valueField.setPromptText("<value>");
            valueField.getStyleClass().add("dialog-text-field");
            valueField.setPrefWidth(200);

            removeBtn = new Button("\u2212");
            removeBtn.getStyleClass().add("remove-param-button");
            removeBtn.setVisible(false);
            removeBtn.setManaged(false);
            removeBtn.setOnAction(e -> removePropertyRow(this));

            container.getChildren().addAll(keyField, valueField, removeBtn);
        }
    }
}
