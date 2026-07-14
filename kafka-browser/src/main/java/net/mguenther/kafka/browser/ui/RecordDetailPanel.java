package net.mguenther.kafka.browser.ui;

import javafx.scene.layout.StackPane;
import net.mguenther.kafka.browser.service.KafkaBrowserService;

/**
 * Container for the right content panel. Switches between:
 * - RecordTableView (table with pagination/seek)
 * - RecordSearchView (search with expandable result cards)
 */
public class RecordDetailPanel extends StackPane {

    private KafkaBrowserService service;
    private RecordTableView tableView;
    private RecordSearchView searchView;
    private String currentTopic;

    public RecordDetailPanel(KafkaBrowserService service) {
        this.service = service;
        getStyleClass().add("record-detail-panel");

        tableView = new RecordTableView(service);
        searchView = new RecordSearchView(service);

        getChildren().addAll(tableView, searchView);
        showTableView();
    }

    public void showTopic(String topicName) {
        this.currentTopic = topicName;
        tableView.loadTopic(topicName);
        searchView.setTopic(topicName);
        showTableView();
    }

    public void showTableView() {
        tableView.setVisible(true);
        tableView.setManaged(true);
        searchView.setVisible(false);
        searchView.setManaged(false);
    }

    public void showSearchView() {
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
