package net.mguenther.kafka.browser.ui;

import javafx.scene.control.ContextMenu;
import javafx.scene.control.MenuItem;
import javafx.scene.control.SeparatorMenuItem;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;

import java.util.Optional;

/**
 * Context menu shown when clicking the environment selector. Structure:
 * - Activate Environment (section)
 *   - List of available environments (e.g., "Use Localhost", "Use Integration")
 *   - No Environment
 * - Manage Environments (section)
 *   - Edit Configuration
 */
public class EnvironmentMenu extends ContextMenu {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onChanged;

    public EnvironmentMenu(BrowserConfig config, ConfigPersistence persistence, Runnable onChanged) {
        this.config = config;
        this.persistence = persistence;
        this.onChanged = onChanged;

        buildMenu();
    }

    private void buildMenu() {
        Workspace active = config.getActiveWorkspace();

        // Activate Environment section
        MenuItem activateHeader = new MenuItem("Activate Environment");
        activateHeader.setDisable(true);
        activateHeader.getStyleClass().add("menu-section-header");
        getItems().add(activateHeader);

        if (active != null) {
            for (Environment env : active.getEnvironments()) {
                MenuItem envItem = new MenuItem("Use " + env.getName());
                envItem.setOnAction(e -> activateEnvironment(env));
                getItems().add(envItem);
            }
        }

        MenuItem noEnvItem = new MenuItem("No Environment");
        noEnvItem.setOnAction(e -> {
            config.setActiveEnvironmentName(null);
            persistence.save(config);
            onChanged.run();
        });
        getItems().add(noEnvItem);

        getItems().add(new SeparatorMenuItem());

        // Manage Environments section
        MenuItem manageHeader = new MenuItem("Manage Environments");
        manageHeader.setDisable(true);
        manageHeader.getStyleClass().add("menu-section-header");
        getItems().add(manageHeader);

        MenuItem editConfigItem = new MenuItem("Edit Configuration");
        editConfigItem.setOnAction(e -> editEnvironmentConfig());
        getItems().add(editConfigItem);
    }

    private void activateEnvironment(Environment env) {
        config.setActiveEnvironmentName(env.getName());
        persistence.save(config);
        onChanged.run();
    }

    private void editEnvironmentConfig() {
        Environment active = config.getActiveEnvironment();
        if (active == null) {
            // Create a default environment if none exists
            Workspace ws = config.getActiveWorkspace();
            if (ws == null) return;
            active = new Environment("Global");
            ws.addEnvironment(active);
            config.setActiveEnvironmentName("Global");
        }

        CustomizeEnvironmentDialog dialog = new CustomizeEnvironmentDialog(active);
        Optional<Environment> result = dialog.showAndWait();
        result.ifPresent(updated -> {
            Workspace ws = config.getActiveWorkspace();
            if (ws != null) {
                // Replace the environment in the workspace
                ws.getEnvironments().stream()
                        .filter(e -> e.getName().equals(updated.getName()))
                        .findFirst()
                        .ifPresent(existing -> existing.setParameters(updated.getParameters()));
                persistence.save(config);
                onChanged.run();
            }
        });
    }
}
