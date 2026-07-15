package net.mguenther.kafka.browser.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import javafx.geometry.Bounds;
import javafx.geometry.Insets;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Popup;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

import java.util.HashMap;

/**
 * Custom styled dropdown menu for the workspace selector.
 * Renders as a Popup with a VBox styled to match the application theme,
 * positioned directly below the workspace header area.
 * Opens overlay dialogs within the provided overlay container.
 */
public class WorkspaceMenu extends Popup {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onChanged;
    private final StackPane overlayContainer;

    public WorkspaceMenu(BrowserConfig config, ConfigPersistence persistence,
                         StackPane overlayContainer, Runnable onChanged) {
        this.config = config;
        this.persistence = persistence;
        this.overlayContainer = overlayContainer;
        this.onChanged = onChanged;

        setAutoHide(true);
        setAutoFix(true);

        VBox content = buildContent();
        getContent().add(content);
    }

    private VBox buildContent() {
        VBox box = new VBox();
        box.getStyleClass().add("dropdown-menu");
        box.setPadding(new Insets(8, 0, 8, 0));
        box.setMinWidth(220);

        Workspace active = config.getActiveWorkspace();

        // Current workspace section
        if (active != null) {
            Label header = createSectionHeader(active.getName());
            box.getChildren().add(header);

            Label settingsItem = createMenuItem("Workspace Settings");
            settingsItem.setOnMouseClicked(e -> { hide(); editWorkspace(active); });
            box.getChildren().add(settingsItem);

            box.getChildren().add(createSeparator());
        }

        // Switch Workspace section
        Label switchHeader = createSectionHeader("Switch Workspace");
        box.getChildren().add(switchHeader);

        for (Workspace ws : config.getWorkspaces()) {
            if (active != null && ws.getName().equals(active.getName())) continue;
            Label wsItem = createMenuItem("To " + ws.getName());
            wsItem.setOnMouseClicked(e -> { hide(); switchToWorkspace(ws); });
            box.getChildren().add(wsItem);
        }

        Label newWsItem = createMenuItem("New Workspace");
        newWsItem.setOnMouseClicked(e -> { hide(); createNewWorkspace(); });
        box.getChildren().add(newWsItem);

        box.getChildren().add(createSeparator());

        // Manage Workspaces section
        Label manageHeader = createSectionHeader("Manage Workspaces");
        box.getChildren().add(manageHeader);

        Label editConfigItem = createMenuItem("Edit Configuration");
        editConfigItem.setOnMouseClicked(e -> { hide(); showManageWorkspaces(); });
        box.getChildren().add(editConfigItem);

        box.getChildren().add(createSeparator());

        // General section
        Label generalHeader = createSectionHeader("General");
        box.getChildren().add(generalHeader);

        Label prefsItem = createMenuItem("Preferences");
        prefsItem.setOnMouseClicked(e -> { hide(); showPreferences(); });
        box.getChildren().add(prefsItem);

        Label importExportItem = createMenuItem("Import / Export");
        importExportItem.setOnMouseClicked(e -> { hide(); showImportExport(); });
        box.getChildren().add(importExportItem);

        return box;
    }

    private Label createSectionHeader(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("dropdown-section-header");
        label.setPadding(new Insets(6, 16, 2, 16));
        label.setMaxWidth(Double.MAX_VALUE);
        return label;
    }

    private Label createMenuItem(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("dropdown-menu-item");
        label.setPadding(new Insets(6, 16, 6, 16));
        label.setMaxWidth(Double.MAX_VALUE);
        label.setCursor(javafx.scene.Cursor.HAND);
        return label;
    }

    private Separator createSeparator() {
        Separator sep = new Separator();
        sep.getStyleClass().add("dropdown-separator");
        VBox.setMargin(sep, new Insets(4, 0, 4, 0));
        return sep;
    }

    public void showBelow(Node anchor) {
        Bounds bounds = anchor.localToScreen(anchor.getBoundsInLocal());
        if (bounds != null) {
            show(anchor, bounds.getMinX(), bounds.getMaxY());
        }
    }

    private void switchToWorkspace(Workspace ws) {
        config.setActiveWorkspaceName(ws.getName());
        if (!ws.getEnvironments().isEmpty()) {
            config.setActiveEnvironmentName(ws.getEnvironments().get(0).getName());
        } else {
            config.setActiveEnvironmentName(null);
        }
        persistence.save(config);
        onChanged.run();
    }

    private void createNewWorkspace() {
        CreateWorkspaceDialog dialog = new CreateWorkspaceDialog();
        dialog.setOnResult(ws -> {
            if (ws != null) {
                ws.addEnvironment(new Environment("Global"));
                config.addWorkspace(ws);
                config.setActiveWorkspaceName(ws.getName());
                config.setActiveEnvironmentName("Global");
                persistence.save(config);
                onChanged.run();
            }
        });
        dialog.showIn(overlayContainer);
    }

    private void editWorkspace(Workspace active) {
        CreateWorkspaceDialog dialog = new CreateWorkspaceDialog(active);
        dialog.setOnResult(updated -> {
            if (updated != null) {
                active.setName(updated.getName());
                active.setBootstrapServers(updated.getBootstrapServers());
                active.setZookeeperConnectUrl(updated.getZookeeperConnectUrl());
                active.setKafkaVersion(updated.getKafkaVersion());
                config.setActiveWorkspaceName(active.getName());
                persistence.save(config);
                onChanged.run();
            }
        });
        dialog.showIn(overlayContainer);
    }

    private void showManageWorkspaces() {
        ManageWorkspacesDialog dialog = new ManageWorkspacesDialog(config, () -> {
            persistence.save(config);
            onChanged.run();
        });
        dialog.showIn(overlayContainer);
    }

    private void showPreferences() {
        PreferencesDialog dialog = new PreferencesDialog(new HashMap<>());
        dialog.setOnResult(prefs -> {
            if (prefs != null) {
                // TODO: persist preferences
            }
        });
        dialog.showIn(overlayContainer);
    }

    private void showImportExport() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            String json = mapper.writeValueAsString(config);
            ImportExportDialog dialog = new ImportExportDialog(ImportExportDialog.Mode.EXPORT, json);
            dialog.showIn(overlayContainer);
        } catch (Exception ex) {
            // Fallback: show empty
            ImportExportDialog dialog = new ImportExportDialog(ImportExportDialog.Mode.EXPORT, "{}");
            dialog.showIn(overlayContainer);
        }
    }
}
