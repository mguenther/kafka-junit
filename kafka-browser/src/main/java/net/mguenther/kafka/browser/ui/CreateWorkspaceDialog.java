package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.Workspace;

/**
 * In-app overlay dialog for creating or editing a workspace.
 * A workspace is project-oriented:
 * - Workspace Name (e.g., "Order Service")
 * - Topic Filter (comma-separated glob patterns, e.g., "order-*,payment-*")
 *
 * Connection settings are managed per environment, not per workspace.
 */
public class CreateWorkspaceDialog extends OverlayDialog<Workspace> {

    private TextField nameField;
    private TextField topicFilterField;

    public CreateWorkspaceDialog() {
        this(null);
    }

    public CreateWorkspaceDialog(Workspace existing) {
        super();

        dialogPane.setMaxWidth(550);

        // Title
        Label title = new Label(existing == null ? "Create Workspace" : "Edit Workspace");
        title.getStyleClass().add("overlay-dialog-title");

        // Workspace Name
        Label nameLabel = new Label("Workspace Name");
        nameLabel.getStyleClass().add("dialog-field-label");
        nameField = new TextField();
        nameField.setPromptText("Order Service");
        nameField.getStyleClass().add("dialog-text-field");

        // Topic Filter
        Label topicFilterLabel = new Label("Topic Filter");
        topicFilterLabel.getStyleClass().add("dialog-field-label");

        Label topicFilterHint = new Label("Comma-separated glob patterns. Leave empty to show all topics.");
        topicFilterHint.getStyleClass().add("dialog-field-hint");
        topicFilterHint.setWrapText(true);

        topicFilterField = new TextField();
        topicFilterField.setPromptText("order-*, payment-*");
        topicFilterField.getStyleClass().add("dialog-text-field");

        // Pre-fill if editing
        if (existing != null) {
            nameField.setText(existing.getName());
            topicFilterField.setText(existing.getTopicFilter());
        }

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button saveBtn = new Button("Save");
        saveBtn.getStyleClass().add("dialog-save-button");
        saveBtn.setOnAction(e -> {
            String name = nameField.getText().trim();
            if (name.isEmpty()) {
                nameField.requestFocus();
                return;
            }
            Workspace ws = new Workspace(name, topicFilterField.getText().trim());
            if (existing != null) {
                // Preserve existing environments
                ws.setEnvironments(existing.getEnvironments());
            }
            close(ws);
        });

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);

        dialogPane.setSpacing(10);
        dialogPane.getChildren().addAll(
                title,
                nameLabel, nameField,
                topicFilterLabel, topicFilterHint, topicFilterField,
                buttonRow
        );
    }
}
