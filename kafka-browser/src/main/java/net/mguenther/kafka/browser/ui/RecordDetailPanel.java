package net.mguenther.kafka.browser.ui;

import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.service.KafkaBrowserService;

/**
 * Container for the right content panel. Shows:
 * - A placeholder message until a topic is selected
 * - Then switches between RecordTableView and RecordSearchView
 * - Remembers the active view when switching topics
 */
public class RecordDetailPanel extends StackPane {

    private enum ActiveView { TABLE, SEARCH }

    private KafkaBrowserService service;
    private RecordTableView tableView;
    private RecordSearchView searchView;
    private VBox placeholder;
    private String currentTopic;
    private ActiveView activeView = ActiveView.TABLE;

    public RecordDetailPanel(KafkaBrowserService service) {
        this.service = service;
        getStyleClass().add("record-detail-panel");

        // Placeholder shown when no topic is selected
        placeholder = new VBox();
        placeholder.setAlignment(Pos.CENTER);
        Label hint = new Label("Select a topic to browse.");
        hint.getStyleClass().add("record-panel-placeholder");
        placeholder.getChildren().add(hint);

        // Views (created but hidden until topic is selected)
        tableView = new RecordTableView(service);
        tableView.setVisible(false);
        tableView.setManaged(false);

        searchView = new RecordSearchView(service);
        searchView.setVisible(false);
        searchView.setManaged(false);

        getChildren().addAll(placeholder, tableView, searchView);
    }

    public void showTopic(String topicName) {
        this.currentTopic = topicName;
        placeholder.setVisible(false);
        placeholder.setManaged(false);
        tableView.loadTopic(topicName);
        searchView.setTopic(topicName);

        // Stay in the currently active view
        if (activeView == ActiveView.SEARCH) {
            showSearchView();
        } else {
            showTableView();
        }
    }

    public void showTableView() {
        activeView = ActiveView.TABLE;
        if (currentTopic == null) return;
        tableView.setVisible(true);
        tableView.setManaged(true);
        searchView.setVisible(false);
        searchView.setManaged(false);
    }

    public void showSearchView() {
        activeView = ActiveView.SEARCH;
        if (currentTopic == null) return;
        tableView.setVisible(false);
        tableView.setManaged(false);
        searchView.setVisible(true);
        searchView.setManaged(true);
    }

    public void setService(KafkaBrowserService service) {
        this.service = service;
        tableView.setService(service);
        searchView.setService(service);
    }
}
