package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

import java.util.HashMap;
import java.util.Map;

/**
 * In-app overlay dialog for application preferences.
 * Settings:
 * - Default page size for record browsing
 * - Poll timeout (seconds)
 * - Auto-detect topic format on selection
 * - Theme (currently only dark)
 */
public class PreferencesDialog extends OverlayDialog<Map<String, String>> {

    private Spinner<Integer> pageSizeSpinner;
    private Spinner<Integer> pollTimeoutSpinner;
    private CheckBox autoDetectFormatCheckBox;

    public PreferencesDialog(Map<String, String> currentPrefs) {
        super();

        dialogPane.setMaxWidth(500);

        // Title
        Label title = new Label("Preferences");
        title.getStyleClass().add("overlay-dialog-title");

        // Page size
        Label pageSizeLabel = new Label("Records per page");
        pageSizeLabel.getStyleClass().add("dialog-field-label");
        pageSizeSpinner = new Spinner<>();
        pageSizeSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(5, 100, parseIntOr(currentPrefs, "pageSize", 12)));
        pageSizeSpinner.setEditable(true);
        pageSizeSpinner.getStyleClass().add("dialog-spinner");
        pageSizeSpinner.setPrefWidth(100);

        HBox pageSizeRow = new HBox(15);
        pageSizeRow.setAlignment(Pos.CENTER_LEFT);
        pageSizeRow.getChildren().addAll(pageSizeLabel, pageSizeSpinner);

        // Poll timeout
        Label pollTimeoutLabel = new Label("Poll timeout (seconds)");
        pollTimeoutLabel.getStyleClass().add("dialog-field-label");
        pollTimeoutSpinner = new Spinner<>();
        pollTimeoutSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 30, parseIntOr(currentPrefs, "pollTimeout", 5)));
        pollTimeoutSpinner.setEditable(true);
        pollTimeoutSpinner.getStyleClass().add("dialog-spinner");
        pollTimeoutSpinner.setPrefWidth(100);

        HBox pollTimeoutRow = new HBox(15);
        pollTimeoutRow.setAlignment(Pos.CENTER_LEFT);
        pollTimeoutRow.getChildren().addAll(pollTimeoutLabel, pollTimeoutSpinner);

        // Auto-detect format
        autoDetectFormatCheckBox = new CheckBox("Auto-detect topic format on selection");
        autoDetectFormatCheckBox.getStyleClass().add("dialog-checkbox");
        autoDetectFormatCheckBox.setSelected(!"false".equals(currentPrefs.getOrDefault("autoDetectFormat", "true")));

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(20, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button saveBtn = new Button("Save");
        saveBtn.getStyleClass().add("dialog-save-button");
        saveBtn.setOnAction(e -> {
            Map<String, String> prefs = new HashMap<>();
            prefs.put("pageSize", String.valueOf(pageSizeSpinner.getValue()));
            prefs.put("pollTimeout", String.valueOf(pollTimeoutSpinner.getValue()));
            prefs.put("autoDetectFormat", String.valueOf(autoDetectFormatCheckBox.isSelected()));
            close(prefs);
        });

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);

        dialogPane.setSpacing(12);
        dialogPane.getChildren().addAll(
                title,
                pageSizeRow,
                pollTimeoutRow,
                autoDetectFormatCheckBox,
                buttonRow
        );
    }

    private int parseIntOr(Map<String, String> prefs, String key, int defaultValue) {
        String val = prefs.get(key);
        if (val == null) return defaultValue;
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
