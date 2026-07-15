package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextArea;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.VBox;

/**
 * In-app overlay dialog for importing/exporting workspace configuration.
 * Allows users to:
 * - Export: view the current configuration as JSON (copy to clipboard)
 * - Import: paste JSON configuration to load workspaces
 */
public class ImportExportDialog extends OverlayDialog<String> {

    public enum Mode { IMPORT, EXPORT }

    private TextArea textArea;

    public ImportExportDialog(Mode mode, String currentConfigJson) {
        super();

        dialogPane.setMaxWidth(650);
        dialogPane.setMinHeight(400);

        // Title
        Label title = new Label(mode == Mode.EXPORT ? "Export Configuration" : "Import Configuration");
        title.getStyleClass().add("overlay-dialog-title");

        // Description
        Label description = new Label(mode == Mode.EXPORT
                ? "Copy the JSON below to save your workspace configuration."
                : "Paste a JSON configuration below to import workspaces.");
        description.getStyleClass().add("dialog-description");
        description.setWrapText(true);

        // Text area
        textArea = new TextArea();
        textArea.getStyleClass().add("dialog-text-area");
        textArea.setPrefRowCount(15);
        textArea.setWrapText(true);
        VBox.setVgrow(textArea, Priority.ALWAYS);

        if (mode == Mode.EXPORT) {
            textArea.setText(currentConfigJson);
            textArea.setEditable(false);
        } else {
            textArea.setPromptText("Paste configuration JSON here...");
        }

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        if (mode == Mode.EXPORT) {
            Button copyBtn = new Button("Copy to Clipboard");
            copyBtn.getStyleClass().add("dialog-save-button");
            copyBtn.setOnAction(e -> {
                javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
                javafx.scene.input.ClipboardContent clipboardContent = new javafx.scene.input.ClipboardContent();
                clipboardContent.putString(textArea.getText());
                clipboard.setContent(clipboardContent);
                copyBtn.setText("Copied!");
            });

            Button closeBtn = new Button("Close");
            closeBtn.getStyleClass().add("dialog-cancel-button");
            closeBtn.setOnAction(e -> close(null));

            buttonRow.getChildren().addAll(closeBtn, copyBtn);
        } else {
            Button cancelBtn = new Button("Cancel");
            cancelBtn.getStyleClass().add("dialog-cancel-button");
            cancelBtn.setOnAction(e -> close(null));

            Button importBtn = new Button("Import");
            importBtn.getStyleClass().add("dialog-save-button");
            importBtn.setOnAction(e -> close(textArea.getText()));

            buttonRow.getChildren().addAll(cancelBtn, importBtn);
        }

        dialogPane.setSpacing(10);
        dialogPane.getChildren().addAll(title, description, textArea, buttonRow);
    }
}
