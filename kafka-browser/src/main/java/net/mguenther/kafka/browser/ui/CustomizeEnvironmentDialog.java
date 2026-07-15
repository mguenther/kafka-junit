package net.mguenther.kafka.browser.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import net.mguenther.kafka.browser.model.Environment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-app overlay dialog for creating or editing an environment.
 * An environment defines a connection target with:
 * - Name, Bootstrap Servers, Kafka Version, ZooKeeper URL
 * - Security settings (protocol, SASL, SSL)
 * - Additional properties
 */
public class CustomizeEnvironmentDialog extends OverlayDialog<Environment> {

    private TextField nameField;
    private TextField bootstrapField;
    private ComboBox<String> versionSelector;
    private VBox zookeeperRow;
    private TextField zookeeperField;
    private ComboBox<String> securityProtocolCombo;
    private VBox saslSection;
    private ComboBox<String> saslMechanismCombo;
    private TextArea saslJaasField;
    private VBox sslSection;
    private TextField sslTruststoreLocationField;
    private PasswordField sslTruststorePasswordField;
    private TextField sslKeystoreLocationField;
    private PasswordField sslKeystorePasswordField;
    private VBox additionalPropsContainer;
    private final List<PropertyRow> propertyRows = new ArrayList<>();

    public CustomizeEnvironmentDialog(Environment existing) {
        super();

        dialogPane.setMaxWidth(650);
        dialogPane.setSpacing(10);

        // Title
        Label title = new Label(existing == null ? "Create Environment" : "Edit Environment");
        title.getStyleClass().add("overlay-dialog-title");

        // Name
        Label nameLabel = new Label("Environment Name");
        nameLabel.getStyleClass().add("dialog-field-label");
        nameField = new TextField(existing != null ? existing.getName() : "");
        nameField.setPromptText("Local");
        nameField.getStyleClass().add("dialog-text-field");

        // Bootstrap Servers
        Label bootstrapLabel = new Label("Bootstrap Servers");
        bootstrapLabel.getStyleClass().add("dialog-field-label");
        bootstrapField = new TextField(existing != null ? existing.getBootstrapServers() : "");
        bootstrapField.setPromptText("localhost:9092");
        bootstrapField.getStyleClass().add("dialog-text-field");

        // Kafka Version
        Label versionLabel = new Label("Kafka Version");
        versionLabel.getStyleClass().add("dialog-field-label");
        versionSelector = new ComboBox<>();
        versionSelector.getItems().addAll("3.x (KRaft)", "3.x (ZooKeeper)", "2.x (ZooKeeper)", "1.x (ZooKeeper)");
        versionSelector.setValue(existing != null ? existing.getKafkaVersion() : "3.x (KRaft)");
        versionSelector.setMaxWidth(Double.MAX_VALUE);
        versionSelector.getStyleClass().add("dialog-combo-box");
        versionSelector.setOnAction(e -> updateZookeeperVisibility());

        // ZooKeeper
        Label zookeeperLabel = new Label("ZooKeeper Connect URL");
        zookeeperLabel.getStyleClass().add("dialog-field-label");
        zookeeperField = new TextField(existing != null ? existing.getZookeeperConnectUrl() : "");
        zookeeperField.setPromptText("localhost:2181");
        zookeeperField.getStyleClass().add("dialog-text-field");
        zookeeperRow = new VBox(5, zookeeperLabel, zookeeperField);
        zookeeperRow.setVisible(false);
        zookeeperRow.setManaged(false);

        // --- Security Section ---
        Separator secSep = new Separator();
        VBox.setMargin(secSep, new Insets(8, 0, 4, 0));

        Label secTitle = new Label("Security");
        secTitle.getStyleClass().add("dialog-field-label");

        // Security Protocol
        Label protocolLabel = new Label("Security Protocol");
        protocolLabel.getStyleClass().add("dialog-field-hint");
        securityProtocolCombo = new ComboBox<>();
        securityProtocolCombo.getItems().addAll("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL");
        securityProtocolCombo.setValue(existing != null ? existing.getSecurityProtocol() : "PLAINTEXT");
        securityProtocolCombo.setMaxWidth(Double.MAX_VALUE);
        securityProtocolCombo.getStyleClass().add("dialog-combo-box");
        securityProtocolCombo.setOnAction(e -> updateSecurityVisibility());

        // SASL section
        saslSection = new VBox(8);
        saslSection.setVisible(false);
        saslSection.setManaged(false);

        Label saslMechLabel = new Label("SASL Mechanism");
        saslMechLabel.getStyleClass().add("dialog-field-hint");
        saslMechanismCombo = new ComboBox<>();
        saslMechanismCombo.getItems().addAll("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER");
        saslMechanismCombo.setValue(existing != null && existing.getSaslMechanism() != null ? existing.getSaslMechanism() : "PLAIN");
        saslMechanismCombo.setMaxWidth(Double.MAX_VALUE);
        saslMechanismCombo.getStyleClass().add("dialog-combo-box");

        Label jaasLabel = new Label("SASL JAAS Config");
        jaasLabel.getStyleClass().add("dialog-field-hint");
        saslJaasField = new TextArea(existing != null && existing.getSaslJaasConfig() != null ? existing.getSaslJaasConfig() : "");
        saslJaasField.setPromptText("org.apache.kafka.common.security.plain.PlainLoginModule required ...");
        saslJaasField.getStyleClass().add("dialog-text-area");
        saslJaasField.setPrefRowCount(3);

        saslSection.getChildren().addAll(saslMechLabel, saslMechanismCombo, jaasLabel, saslJaasField);

        // SSL section
        sslSection = new VBox(8);
        sslSection.setVisible(false);
        sslSection.setManaged(false);

        Label truststoreLocLabel = new Label("Truststore Location");
        truststoreLocLabel.getStyleClass().add("dialog-field-hint");
        sslTruststoreLocationField = new TextField(existing != null ? existing.getSslTruststoreLocation() : "");
        sslTruststoreLocationField.setPromptText("/path/to/truststore.jks");
        sslTruststoreLocationField.getStyleClass().add("dialog-text-field");

        Label truststorePwLabel = new Label("Truststore Password");
        truststorePwLabel.getStyleClass().add("dialog-field-hint");
        sslTruststorePasswordField = new PasswordField();
        sslTruststorePasswordField.setText(existing != null ? existing.getSslTruststorePassword() : "");
        sslTruststorePasswordField.getStyleClass().add("dialog-text-field");

        Label keystoreLocLabel = new Label("Keystore Location (mTLS)");
        keystoreLocLabel.getStyleClass().add("dialog-field-hint");
        sslKeystoreLocationField = new TextField(existing != null ? existing.getSslKeystoreLocation() : "");
        sslKeystoreLocationField.setPromptText("/path/to/keystore.jks");
        sslKeystoreLocationField.getStyleClass().add("dialog-text-field");

        Label keystorePwLabel = new Label("Keystore Password");
        keystorePwLabel.getStyleClass().add("dialog-field-hint");
        sslKeystorePasswordField = new PasswordField();
        sslKeystorePasswordField.setText(existing != null ? existing.getSslKeystorePassword() : "");
        sslKeystorePasswordField.getStyleClass().add("dialog-text-field");

        sslSection.getChildren().addAll(
                truststoreLocLabel, sslTruststoreLocationField,
                truststorePwLabel, sslTruststorePasswordField,
                keystoreLocLabel, sslKeystoreLocationField,
                keystorePwLabel, sslKeystorePasswordField
        );

        // --- Additional Properties ---
        Separator propsSep = new Separator();
        VBox.setMargin(propsSep, new Insets(8, 0, 4, 0));

        HBox additionalLabelRow = new HBox(8);
        Label additionalLabel = new Label("Additional Properties");
        additionalLabel.getStyleClass().add("dialog-field-label-optional");
        Label optionalBadge = new Label("optional");
        optionalBadge.getStyleClass().add("optional-badge");
        additionalLabelRow.getChildren().addAll(additionalLabel, optionalBadge);

        additionalPropsContainer = new VBox(5);
        if (existing != null && existing.getAdditionalProperties() != null) {
            for (Map.Entry<String, String> entry : existing.getAdditionalProperties().entrySet()) {
                addPropertyRow(entry.getKey(), entry.getValue());
            }
        }
        addEmptyPropertyRow();

        // Buttons
        HBox buttonRow = new HBox(10);
        buttonRow.setAlignment(Pos.CENTER_RIGHT);
        buttonRow.setPadding(new Insets(15, 0, 0, 0));

        Button cancelBtn = new Button("Cancel");
        cancelBtn.getStyleClass().add("dialog-cancel-button");
        cancelBtn.setOnAction(e -> close(null));

        Button saveBtn = new Button("Save");
        saveBtn.getStyleClass().add("dialog-save-button");
        saveBtn.setOnAction(e -> saveAndClose());

        buttonRow.getChildren().addAll(cancelBtn, saveBtn);

        dialogPane.getChildren().addAll(
                title,
                nameLabel, nameField,
                bootstrapLabel, bootstrapField,
                versionLabel, versionSelector,
                zookeeperRow,
                secSep, secTitle,
                protocolLabel, securityProtocolCombo,
                saslSection,
                sslSection,
                propsSep, additionalLabelRow, additionalPropsContainer,
                buttonRow
        );

        updateZookeeperVisibility();
        updateSecurityVisibility();
    }

    private void updateZookeeperVisibility() {
        String version = versionSelector.getValue();
        boolean showZk = version != null && version.contains("ZooKeeper");
        zookeeperRow.setVisible(showZk);
        zookeeperRow.setManaged(showZk);
    }

    private void updateSecurityVisibility() {
        String protocol = securityProtocolCombo.getValue();
        boolean showSasl = protocol != null && protocol.contains("SASL");
        boolean showSsl = protocol != null && (protocol.contains("SSL"));

        saslSection.setVisible(showSasl);
        saslSection.setManaged(showSasl);
        sslSection.setVisible(showSsl);
        sslSection.setManaged(showSsl);
    }

    private void saveAndClose() {
        String name = nameField.getText().trim();
        if (name.isEmpty()) {
            nameField.requestFocus();
            return;
        }

        Map<String, String> additionalProps = new LinkedHashMap<>();
        for (PropertyRow row : propertyRows) {
            String key = row.keyField.getText().trim();
            String value = row.valueField.getText().trim();
            if (!key.isEmpty()) {
                additionalProps.put(key, value);
            }
        }

        Environment env = new Environment(
                name,
                safeText(bootstrapField),
                safeText(zookeeperField),
                versionSelector.getValue(),
                securityProtocolCombo.getValue(),
                saslMechanismCombo.getValue(),
                safeText(saslJaasField),
                safeText(sslTruststoreLocationField),
                safeText(sslTruststorePasswordField),
                safeText(sslKeystoreLocationField),
                safeText(sslKeystorePasswordField),
                additionalProps
        );
        close(env);
    }

    private String safeText(javafx.scene.control.TextInputControl field) {
        String text = field.getText();
        return text != null ? text.trim() : "";
    }

    private void addPropertyRow(String key, String value) {
        PropertyRow row = new PropertyRow(key, value, true);
        propertyRows.add(row);
        additionalPropsContainer.getChildren().add(row.container);
    }

    private void addEmptyPropertyRow() {
        PropertyRow row = new PropertyRow("", "", false);
        additionalPropsContainer.getChildren().add(row.container);
        row.keyField.textProperty().addListener((obs, oldVal, newVal) -> {
            if (!newVal.isEmpty() && !propertyRows.contains(row)) {
                propertyRows.add(row);
                row.removeBtn.setVisible(true);
                row.removeBtn.setManaged(true);
                addEmptyPropertyRow();
            }
        });
    }

    private void removePropertyRow(PropertyRow row) {
        propertyRows.remove(row);
        additionalPropsContainer.getChildren().remove(row.container);
    }

    private class PropertyRow {
        HBox container;
        TextField keyField;
        TextField valueField;
        Button removeBtn;

        PropertyRow(String key, String value, boolean showRemove) {
            container = new HBox(10);
            container.setAlignment(Pos.CENTER_LEFT);

            keyField = new TextField(key);
            keyField.setPromptText("<property>");
            keyField.getStyleClass().add("dialog-text-field");
            keyField.setPrefWidth(220);

            valueField = new TextField(value);
            valueField.setPromptText("<value>");
            valueField.getStyleClass().add("dialog-text-field");
            valueField.setPrefWidth(220);

            removeBtn = new Button("\u2212");
            removeBtn.getStyleClass().add("remove-param-button");
            removeBtn.setVisible(showRemove);
            removeBtn.setManaged(showRemove);
            removeBtn.setOnAction(e -> removePropertyRow(this));

            container.getChildren().addAll(keyField, valueField, removeBtn);
        }
    }
}
