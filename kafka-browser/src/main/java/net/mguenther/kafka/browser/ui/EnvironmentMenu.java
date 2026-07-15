package net.mguenther.kafka.browser.ui;

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

import java.util.ArrayList;
import java.util.List;

/**
 * Custom styled dropdown menu for the environment selector.
 * Renders as a Popup with a VBox styled to match the application theme,
 * positioned directly below the environment header area.
 * Opens overlay dialogs within the provided overlay container.
 */
public class EnvironmentMenu extends Popup {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;
    private final Runnable onChanged;
    private final StackPane overlayContainer;

    public EnvironmentMenu(BrowserConfig config, ConfigPersistence persistence,
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

        Workspace activeWs = config.getActiveWorkspace();
        Environment activeEnv = config.getActiveEnvironment();

        // Current environment section
        if (activeEnv != null) {
            Label header = createSectionHeader(activeEnv.getName());
            box.getChildren().add(header);

            Label settingsItem = createMenuItem("Environment Settings");
            settingsItem.setOnMouseClicked(e -> { hide(); editEnvironmentConfig(); });
            box.getChildren().add(settingsItem);

            box.getChildren().add(createSeparator());
        }

        // Switch Environment section
        Label switchHeader = createSectionHeader("Switch Environment");
        box.getChildren().add(switchHeader);

        if (activeWs != null) {
            for (Environment env : activeWs.getEnvironments()) {
                if (activeEnv != null && env.getName().equals(activeEnv.getName())) continue;
                Label envItem = createMenuItem("To " + env.getName());
                envItem.setOnMouseClicked(e -> { hide(); activateEnvironment(env); });
                box.getChildren().add(envItem);
            }
        }

        Label newEnvItem = createMenuItem("New Environment");
        newEnvItem.setOnMouseClicked(e -> { hide(); createNewEnvironment(); });
        box.getChildren().add(newEnvItem);

        box.getChildren().add(createSeparator());

        // Manage Environments section
        Label manageHeader = createSectionHeader("Manage Environments");
        box.getChildren().add(manageHeader);

        Label editConfigItem = createMenuItem("Edit Configuration");
        editConfigItem.setOnMouseClicked(e -> { hide(); showManageEnvironments(); });
        box.getChildren().add(editConfigItem);

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

    private void activateEnvironment(Environment env) {
        config.setActiveEnvironmentName(env.getName());
        persistence.save(config);
        onChanged.run();
    }

    private void createNewEnvironment() {
        CustomizeEnvironmentDialog dialog = new CustomizeEnvironmentDialog(null);
        dialog.setOnResult(env -> {
            if (env != null) {
                Workspace ws = config.getActiveWorkspace();
                if (ws != null) {
                    ws.addEnvironment(env);
                    config.setActiveEnvironmentName(env.getName());
                    persistence.save(config);
                    onChanged.run();
                }
            }
        });
        dialog.showIn(overlayContainer);
    }

    private void showManageEnvironments() {
        ManageEnvironmentsDialog dialog = new ManageEnvironmentsDialog(config, () -> {
            persistence.save(config);
            onChanged.run();
        });
        dialog.showIn(overlayContainer);
    }

    private void editEnvironmentConfig() {
        Environment active = config.getActiveEnvironment();
        if (active == null) {
            Workspace ws = config.getActiveWorkspace();
            if (ws == null) return;
            active = new Environment("Local", "localhost:9092");
            ws.addEnvironment(active);
            config.setActiveEnvironmentName("Local");
        }

        final Environment envToEdit = active;
        CustomizeEnvironmentDialog dialog = new CustomizeEnvironmentDialog(envToEdit);
        dialog.setOnResult(updated -> {
            if (updated != null) {
                Workspace ws = config.getActiveWorkspace();
                if (ws != null) {
                    // Replace the environment in the workspace's list
                    List<Environment> envs = new ArrayList<>(ws.getEnvironments());
                    for (int i = 0; i < envs.size(); i++) {
                        if (envs.get(i).getName().equals(envToEdit.getName())) {
                            envs.set(i, updated);
                            break;
                        }
                    }
                    ws.setEnvironments(envs);
                    config.setActiveEnvironmentName(updated.getName());
                    persistence.save(config);
                    onChanged.run();
                }
            }
        });
        dialog.showIn(overlayContainer);
    }
}
