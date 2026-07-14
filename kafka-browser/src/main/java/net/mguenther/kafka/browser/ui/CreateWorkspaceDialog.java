package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.Workspace;

/**
 * Dialog for creating a new workspace. Fields:
 * - Workspace Name
 * - Kafka Bootstrap Servers
 * - Kafka Version selector (determines if ZooKeeper field is shown)
 * - ZooKeeper Connect URL (visible for older versions)
 */
public class CreateWorkspaceDialog extends Dialog<Workspace> {

    private TextField nameField;
    private TextField bootstrapField;
    private TextField zookeeperField;
    private ComboBox<String> versionSelector;
    private VBox zookeeperRow;

    public CreateWorkspaceDialog() {
        this(null);
    }

    public CreateWorkspaceDialog(Workspace existing) {
        setTitle(existing == null ? "Create Workspace" : "Edit Workspace");
        setHeaderText(null);

        VBox content = new VBox(15);
        content.setPadding(new Insets(20));
        content.setPrefWidth(550);

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
        versionSelector.getItems().addAll("3.x (KRaft)", "2.x (ZooKeeper)", "1.x (ZooKeeper)");
        versionSelector.getSelectionModel().selectFirst();
        versionSelector.setMaxWidth(Double.MAX_VALUE);
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

        content.getChildren().addAll(
                nameLabel, nameField,
                bootstrapLabel, bootstrapField,
                versionLabel, versionSelector,
                zookeeperRow
        );

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));
        buttonRow.setStyle("-fx-alignment: center-right;");

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> {
            setResult(null);
            close();
        });

        Button saveBtn = new Button("Save");
        saveBtn.getStyleClass().add("dialog-save-button");
        saveBtn.setOnAction(e -> {
            Workspace ws = new Workspace(
                    nameField.getText().trim(),
                    bootstrapField.getText().trim(),
                    zookeeperField.getText().trim(),
                    versionSelector.getValue()
            );
            setResult(ws);
            close();
        });

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);
        content.getChildren().add(buttonRow);

        getDialogPane().setContent(content);
        // Hidden ButtonType to allow programmatic close
        getDialogPane().getButtonTypes().add(ButtonType.CLOSE);
        getDialogPane().lookupButton(ButtonType.CLOSE).setVisible(false);
        getDialogPane().lookupButton(ButtonType.CLOSE).setManaged(false);

        setResultConverter(bt -> null);
    }

    private void updateZookeeperVisibility() {
        String version = versionSelector.getValue();
        boolean showZk = version != null && (version.contains("2.x") || version.contains("1.x"));
        zookeeperRow.setVisible(showZk);
        zookeeperRow.setManaged(showZk);
    }
}
