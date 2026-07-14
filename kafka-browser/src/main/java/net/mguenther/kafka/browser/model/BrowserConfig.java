package net.mguenther.kafka.browser.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Root configuration object that holds all workspaces and the currently
 * active workspace/environment selection. Serialized to JSON for persistence.
 */
public class BrowserConfig {

    private List<Workspace> workspaces;
    private String activeWorkspaceName;
    private String activeEnvironmentName;

    @JsonCreator
    public BrowserConfig(@JsonProperty("workspaces") List<Workspace> workspaces,
                         @JsonProperty("activeWorkspaceName") String activeWorkspaceName,
                         @JsonProperty("activeEnvironmentName") String activeEnvironmentName) {
        this.workspaces = workspaces != null ? new ArrayList<>(workspaces) : new ArrayList<>();
        this.activeWorkspaceName = activeWorkspaceName;
        this.activeEnvironmentName = activeEnvironmentName;
    }

    public BrowserConfig() {
        this(new ArrayList<>(), null, null);
    }

    public List<Workspace> getWorkspaces() {
        return Collections.unmodifiableList(workspaces);
    }

    public void setWorkspaces(List<Workspace> workspaces) {
        this.workspaces = new ArrayList<>(workspaces);
    }

    public void addWorkspace(Workspace workspace) {
        this.workspaces.add(workspace);
    }

    public void removeWorkspace(Workspace workspace) {
        this.workspaces.remove(workspace);
        if (workspace.getName().equals(activeWorkspaceName)) {
            activeWorkspaceName = null;
            activeEnvironmentName = null;
        }
    }

    public String getActiveWorkspaceName() {
        return activeWorkspaceName;
    }

    public void setActiveWorkspaceName(String activeWorkspaceName) {
        this.activeWorkspaceName = activeWorkspaceName;
    }

    public String getActiveEnvironmentName() {
        return activeEnvironmentName;
    }

    public void setActiveEnvironmentName(String activeEnvironmentName) {
        this.activeEnvironmentName = activeEnvironmentName;
    }

    public Workspace getActiveWorkspace() {
        if (activeWorkspaceName == null) return null;
        return workspaces.stream()
                .filter(w -> w.getName().equals(activeWorkspaceName))
                .findFirst()
                .orElse(null);
    }

    public Environment getActiveEnvironment() {
        Workspace ws = getActiveWorkspace();
        if (ws == null || activeEnvironmentName == null) return null;
        return ws.getEnvironments().stream()
                .filter(e -> e.getName().equals(activeEnvironmentName))
                .findFirst()
                .orElse(null);
    }
}
