package net.mguenther.kafka.browser.ui;

import javafx.scene.control.ContextMenu;
import javafx.scene.control.Menu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

import java.util.Optional;

/**
 * Context menu shown when clicking the workspace selector. Structure:
 * - Current workspace name (header)
 *   - Workspace Settings
 * - Switch Workspace (section)
 *   - List of other workspaces
 *   - New Workspace
 * - Manage Workspaces (section)
 *   - Edit Configuration
 * - General (section)
 *   - Preferences
 *   - Import / Export
 */
public class WorkspaceMenu extends ContextMenu {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onChanged;

    public WorkspaceMenu(BrowserConfig config, ConfigPersistence persistence, Runnable onChanged) {
        this.config = config;
        this.persistence = persistence;
        this.onChanged = onChanged;

        buildMenu();
    }

    private void buildMenu() {
        Workspace active = config.getActiveWorkspace();

        // Current workspace section
        if (active != null) {
            MenuItem header = new MenuItem(active.getName());
            header.setDisable(true);
            header.getStyleClass().add("menu-section-header");
            getItems().add(header);

            MenuItem settingsItem = new MenuItem("Workspace Settings");
            settingsItem.setOnAction(e -> editWorkspace(active));
            getItems().add(settingsItem);

            getItems().add(new SeparatorMenuItem());
        }

        // Switch Workspace section
        MenuItem switchHeader = new MenuItem("Switch Workspace");
        switchHeader.setDisable(true);
        switchHeader.getStyleClass().add("menu-section-header");
        getItems().add(switchHeader);

        for (Workspace ws : config.getWorkspaces()) {
            if (active != null && ws.getName().equals(active.getName())) continue;
            MenuItem wsItem = new MenuItem("To " + ws.getName());
            wsItem.setOnAction(e -> switchToWorkspace(ws));
            getItems().add(wsItem);
        }

        MenuItem newWsItem = new MenuItem("New Workspace");
        newWsItem.setOnAction(e -> createNewWorkspace());
        getItems().add(newWsItem);

        getItems().add(new SeparatorMenuItem());

        // Manage Workspaces section
        MenuItem manageHeader = new MenuItem("Manage Workspaces");
        manageHeader.setDisable(true);
        manageHeader.getStyleClass().add("menu-section-header");
        getItems().add(manageHeader);

        MenuItem editConfigItem = new MenuItem("Edit Configuration");
        editConfigItem.setOnAction(e -> editConfiguration());
        getItems().add(editConfigItem);

        getItems().add(new SeparatorMenuItem());

        // General section
        MenuItem generalHeader = new MenuItem("General");
        generalHeader.setDisable(true);
        generalHeader.getStyleClass().add("menu-section-header");
        getItems().add(generalHeader);

        MenuItem prefsItem = new MenuItem("Preferences");
        prefsItem.setOnAction(e -> {/* TODO: preferences */});
        getItems().add(prefsItem);

        MenuItem importExportItem = new MenuItem("Import / Export");
        importExportItem.setOnAction(e -> {/* TODO: import/export */});
        getItems().add(importExportItem);
    }

    private void switchToWorkspace(Workspace ws) {
        config.setActiveWorkspaceName(ws.getName());
        // Default to first environment or null
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
        Optional<Workspace> result = dialog.showAndWait();
        result.ifPresent(ws -> {
            // Add a default "Global" environment
            ws.addEnvironment(new Environment("Global"));
            config.addWorkspace(ws);
            config.setActiveWorkspaceName(ws.getName());
            config.setActiveEnvironmentName("Global");
            persistence.save(config);
            onChanged.run();
        });
    }

    private void editWorkspace(Workspace active) {
        CreateWorkspaceDialog dialog = new CreateWorkspaceDialog(active);
        Optional<Workspace> result = dialog.showAndWait();
        result.ifPresent(updated -> {
            active.setName(updated.getName());
            active.setBootstrapServers(updated.getBootstrapServers());
            active.setZookeeperConnectUrl(updated.getZookeeperConnectUrl());
            active.setKafkaVersion(updated.getKafkaVersion());
            config.setActiveWorkspaceName(active.getName());
            persistence.save(config);
            onChanged.run();
        });
    }

    private void editConfiguration() {
        Workspace active = config.getActiveWorkspace();
        if (active != null) {
            editWorkspace(active);
        }
    }
}
