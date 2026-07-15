package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.Workspace;

/**
 * In-app overlay dialog for creating or editing a workspace.
 * Renders as a centered panel within the content area matching the mockup style.
 * Fields:
 * - Workspace Name
 * - Kafka Bootstrap Servers
 * - Kafka Version selector (determines if ZooKeeper field is shown)
 * - ZooKeeper Connect URL (visible for older versions)
 */
public class CreateWorkspaceDialog extends OverlayDialog<Workspace> {

    private TextField nameField;
    private TextField bootstrapField;
    private TextField zookeeperField;
    private ComboBox<String> versionSelector;
    private VBox zookeeperRow;

    public CreateWorkspaceDialog() {
        this(null);
    }

    public CreateWorkspaceDialog(Workspace existing) {
        super();

        // Title
        Label title = new Label(existing == null ? "Create Workspace" : "Edit Workspace");
        title.getStyleClass().add("overlay-dialog-title");

        // Workspace Name
        Label nameLabel = new Label("Workspace Name");
        nameLabel.getStyleClass().add("dialog-field-label");
        nameField = new TextField();
        nameField.setPromptText("DSGVO Service");
        nameField.getStyleClass().add("dialog-text-field");

        // Kafka Bootstrap Servers
        Label bootstrapLabel = new Label("Kafka Bootstrap Servers");
        bootstrapLabel.getStyleClass().add("dialog-field-label");
        bootstrapField = new TextField();
        bootstrapField.setPromptText("http://localhost:9010");
        bootstrapField.getStyleClass().add("dialog-text-field");

        // Kafka Version
        Label versionLabel = new Label("Kafka Version");
        versionLabel.getStyleClass().add("dialog-field-label");
        versionSelector = new ComboBox<>();
        versionSelector.getItems().addAll("3.x (KRaft)", "3.x (ZooKeeper)", "2.x (ZooKeeper)", "1.x (ZooKeeper)");
        versionSelector.getSelectionModel().selectFirst();
        versionSelector.setMaxWidth(Double.MAX_VALUE);
        versionSelector.getStyleClass().add("dialog-combo-box");
        versionSelector.setOnAction(e -> updateZookeeperVisibility());

        // ZooKeeper Connect URL
        Label zookeeperLabel = new Label("ZooKeeper Connect URL");
        zookeeperLabel.getStyleClass().add("dialog-field-label");
        zookeeperField = new TextField();
        zookeeperField.setPromptText("http://localhost:2181");
        zookeeperField.getStyleClass().add("dialog-text-field");

        zookeeperRow = new VBox(5, zookeeperLabel, zookeeperField);
        zookeeperRow.setVisible(false);
        zookeeperRow.setManaged(false);

        // Pre-fill if editing
        if (existing != null) {
            nameField.setText(existing.getName());
            bootstrapField.setText(existing.getBootstrapServers());
            zookeeperField.setText(existing.getZookeeperConnectUrl());
            String version = existing.getKafkaVersion();
            if (version != null) {
                versionSelector.setValue(version);
            }
            updateZookeeperVisibility();
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
            Workspace ws = new Workspace(
                    nameField.getText().trim(),
                    bootstrapField.getText().trim(),
                    zookeeperField.getText().trim(),
                    versionSelector.getValue()
            );
            close(ws);
        });

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);

        dialogPane.setSpacing(10);
        dialogPane.getChildren().addAll(
                title,
                nameLabel, nameField,
                bootstrapLabel, bootstrapField,
                versionLabel, versionSelector,
                zookeeperRow,
                buttonRow
        );
    }

    private void updateZookeeperVisibility() {
        String version = versionSelector.getValue();
        boolean showZk = version != null && version.contains("ZooKeeper");
        zookeeperRow.setVisible(showZk);
        zookeeperRow.setManaged(showZk);
    }
}
