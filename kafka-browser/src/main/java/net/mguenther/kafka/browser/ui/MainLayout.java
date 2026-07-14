package net.mguenther.kafka.browser.ui;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.BrowserConfig;
import net.mguenther.kafka.browser.model.ConfigPersistence;
import net.mguenther.kafka.browser.model.Workspace;
import net.mguenther.kafka.browser.service.KafkaBrowserService;

/**
 * Main layout implementing the 4-zone structure:
 * - Top: Header bar (workspace selector left, environment selector right)
 * - Left: Icon sidebar (narrow navigation rail)
 * - Center-Left: Topic list panel
 * - Center-Right: Record detail panel (table or search view)
 */
public class MainLayout extends BorderPane {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;

    private HeaderBar headerBar;
    private IconSidebar iconSidebar;
    private TopicListPanel topicListPanel;
    private RecordDetailPanel recordDetailPanel;
    private StackPane contentArea;

    private KafkaBrowserService service;

    public MainLayout(BrowserConfig config, ConfigPersistence persistence) {
        this.config = config;
        this.persistence = persistence;

        getStyleClass().add("main-layout");

        initializeService();
        buildUI();
    }

    private void initializeService() {
        Workspace ws = config.getActiveWorkspace();
        if (ws != null) {
            service = new KafkaBrowserService(ws, config.getActiveEnvironment());
        }
    }

    private void buildUI() {
        // Top: Header bar
        headerBar = new HeaderBar(config, persistence, this::onWorkspaceChanged, this::onEnvironmentChanged);
        setTop(headerBar);

        // Left: Icon sidebar
        iconSidebar = new IconSidebar(this::onSearchClicked, this::onBrowseClicked, this::onInfoClicked);
        setLeft(iconSidebar);

        // Center: Split between topic list and record detail
        contentArea = new StackPane();
        contentArea.getStyleClass().add("content-area");

        if (service != null) {
            buildContentPanels();
        } else {
            showEmptyState();
        }

        setCenter(contentArea);
    }

    private void buildContentPanels() {
        HBox splitContent = new HBox();
        splitContent.getStyleClass().add("split-content");

        topicListPanel = new TopicListPanel(service, this::onTopicSelected);
        topicListPanel.setPrefWidth(400);
        topicListPanel.setMinWidth(300);

        recordDetailPanel = new RecordDetailPanel(service);
        HBox.setHgrow(recordDetailPanel, Priority.ALWAYS);

        splitContent.getChildren().addAll(topicListPanel, recordDetailPanel);
        contentArea.getChildren().setAll(splitContent);
    }

    private void showEmptyState() {
        VBox emptyState = new VBox();
        emptyState.setAlignment(Pos.CENTER);
        emptyState.getStyleClass().add("empty-state");

        StackPane card = new StackPane();
        card.getStyleClass().add("empty-state-card");
        card.setMaxWidth(500);
        card.setMaxHeight(80);

        Label message = new Label("You have not configured a workspace yet.");
        message.getStyleClass().add("empty-state-message");
        card.getChildren().add(message);

        emptyState.getChildren().add(card);
        contentArea.getChildren().setAll(emptyState);
    }

    private void onWorkspaceChanged() {
        initializeService();
        contentArea.getChildren().clear();
        if (service != null) {
            buildContentPanels();
            topicListPanel.refresh();
        } else {
            showEmptyState();
        }
    }

    private void onEnvironmentChanged() {
        initializeService();
        if (service != null && topicListPanel != null) {
            topicListPanel.setService(service);
            recordDetailPanel.setService(service);
            topicListPanel.refresh();
        }
    }

    private void onTopicSelected(String topicName) {
        if (recordDetailPanel != null) {
            recordDetailPanel.showTopic(topicName);
        }
    }

    private void onSearchClicked() {
        if (recordDetailPanel != null) {
            recordDetailPanel.showSearchView();
        }
    }

    private void onBrowseClicked() {
        if (recordDetailPanel != null) {
            recordDetailPanel.showTableView();
        }
    }

    private void onInfoClicked() {
        // Placeholder for info/about panel
    }
}
