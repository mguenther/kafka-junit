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
import net.mguenther.kafka.browser.model.Environment;
import net.mguenther.kafka.browser.model.Workspace;
import net.mguenther.kafka.browser.service.KafkaBrowserService;

/**
 * Main layout implementing the 4-zone structure:
 * - Top: Header bar (workspace selector left, environment selector right)
 * - Left: Icon sidebar (narrow navigation rail)
 * - Center-Left: Topic list panel
 * - Center-Right: Record detail panel (table or search view) or Info panel
 */
public class MainLayout extends BorderPane {

    private final BrowserConfig config;
    private final ConfigPersistence persistence;

    private HeaderBar headerBar;
    private IconSidebar iconSidebar;
    private TopicListPanel topicListPanel;
    private RecordDetailPanel recordDetailPanel;
    private InfoPanel infoPanel;
    private HBox splitContent;
    private StackPane contentArea;

    private KafkaBrowserService service;
    private String selectedTopic;

    public MainLayout(BrowserConfig config, ConfigPersistence persistence) {
        this.config = config;
        this.persistence = persistence;

        getStyleClass().add("main-layout");

        initializeService();
        buildUI();
    }

    private void initializeService() {
        Workspace ws = config.getActiveWorkspace();
        Environment env = config.getActiveEnvironment();
        if (ws != null && env != null && !env.getBootstrapServers().isEmpty()) {
            service = new KafkaBrowserService(env);
        } else {
            service = null;
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

        // Wire overlay container for in-app dialogs
        headerBar.setOverlayContainer(contentArea);

        if (service != null) {
            buildContentPanels();
            iconSidebar.setButtonsDisabled(false);
            topicListPanel.refresh();
        } else {
            showEmptyState();
            iconSidebar.setButtonsDisabled(true);
        }

        setCenter(contentArea);
    }

    private void buildContentPanels() {
        splitContent = new HBox();
        splitContent.getStyleClass().add("split-content");

        Workspace ws = config.getActiveWorkspace();

        topicListPanel = new TopicListPanel(service, ws, this::onTopicSelected);
        topicListPanel.setPrefWidth(400);
        topicListPanel.setMinWidth(300);

        recordDetailPanel = new RecordDetailPanel(service);
        HBox.setHgrow(recordDetailPanel, Priority.ALWAYS);

        infoPanel = new InfoPanel(service, config);
        HBox.setHgrow(infoPanel, Priority.ALWAYS);
        infoPanel.setVisible(false);
        infoPanel.setManaged(false);

        splitContent.getChildren().addAll(topicListPanel, recordDetailPanel, infoPanel);
        contentArea.getChildren().setAll(splitContent);
    }

    private void showEmptyState() {
        VBox emptyState = new VBox();
        emptyState.setAlignment(Pos.CENTER);
        emptyState.getStyleClass().add("empty-state");

        StackPane card = new StackPane();
        card.getStyleClass().add("empty-state-card");
        card.setMaxWidth(550);
        card.setMaxHeight(80);

        Workspace ws = config.getActiveWorkspace();
        String messageText;
        if (ws == null) {
            messageText = "You have not configured a workspace yet.";
        } else if (ws.getEnvironments().isEmpty() || config.getActiveEnvironment() == null) {
            messageText = "No environment configured. Add an environment to connect to a Kafka cluster.";
        } else {
            messageText = "Unable to connect. Check your environment settings.";
        }

        Label message = new Label(messageText);
        message.getStyleClass().add("empty-state-message");
        message.setWrapText(true);
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
            iconSidebar.setButtonsDisabled(false);
            iconSidebar.activateBrowse();
        } else {
            showEmptyState();
            iconSidebar.setButtonsDisabled(true);
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
        this.selectedTopic = topicName;
        if (recordDetailPanel != null) {
            recordDetailPanel.showTopic(topicName);
        }
    }

    private void onSearchClicked() {
        showBrowseLayout();
        if (recordDetailPanel != null) {
            recordDetailPanel.showSearchView();
        }
        if (topicListPanel != null) {
            topicListPanel.refresh();
        }
    }

    private void onBrowseClicked() {
        showBrowseLayout();
        if (recordDetailPanel != null) {
            recordDetailPanel.showTableView();
        }
        if (topicListPanel != null) {
            topicListPanel.refresh();
        }
    }

    private void onInfoClicked() {
        showInfoLayout();
        if (infoPanel != null) {
            infoPanel.loadInfo();
        }
    }

    /**
     * Shows the browse/search layout (topic list + record detail panel).
     */
    private void showBrowseLayout() {
        if (recordDetailPanel == null) return;
        topicListPanel.setVisible(true);
        topicListPanel.setManaged(true);
        recordDetailPanel.setVisible(true);
        recordDetailPanel.setManaged(true);
        if (infoPanel != null) {
            infoPanel.setVisible(false);
            infoPanel.setManaged(false);
        }
    }

    /**
     * Shows the info layout (full width, no topic list).
     */
    private void showInfoLayout() {
        if (infoPanel == null) return;
        topicListPanel.setVisible(false);
        topicListPanel.setManaged(false);
        recordDetailPanel.setVisible(false);
        recordDetailPanel.setManaged(false);
        infoPanel.setVisible(true);
        infoPanel.setManaged(true);
    }
}
