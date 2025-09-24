package com.example.contentful.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;

public class PageComponent {

    public String pageId;
    public String pageType;
    public Boolean pagePublished;
    public List<ResolvedComponent> resolvedComponents;
    
    // metadata - not serialized
    @JsonIgnore
    public List<String> pageComponentIds;
    @JsonIgnore
    public List<String> missingComponentIds;
    @JsonIgnore
    public List<String> circularReferences;
    @JsonIgnore
    public int resolutionDepth;
    @JsonIgnore
    public boolean fullyResolved;
    
    public PageComponent() {
        this.resolvedComponents = new ArrayList<>();
        this.missingComponentIds = new ArrayList<>();
        this.circularReferences = new ArrayList<>();
        this.pageComponentIds = new ArrayList<>();
        this.resolutionDepth = 0;
        this.fullyResolved = false;
    }
    
    public PageComponent(String pageId, String pageType, Boolean pagePublished, List<String> componentIds) {
        this();
        this.pageId = pageId;
        this.pageType = pageType;
        this.pagePublished = pagePublished;
        this.pageComponentIds = new ArrayList<>(componentIds);
    }
    
    public static class ResolvedComponent {
        public String id;
        public String type;
        public String label;
        public String name;
        public Boolean published;
        public List<ResolvedComponent> childComponents;
        public List<String> missingComponentIds; // Track missing components at this level
        
        // internal - not serialized
        @JsonIgnore
        public int depth;
        
        public ResolvedComponent() {
            this.childComponents = new ArrayList<>();
            this.missingComponentIds = new ArrayList<>();
            this.depth = 0;
        }
        
        public ResolvedComponent(Component component, int depth) {
            this();
            this.id = component.id;
            this.type = component.type;
            this.label = component.label;
            this.name = component.name;
            this.published = component.published;
            this.depth = depth;
        }
        
        public void addMissingComponent(String componentId) {
            if (!this.missingComponentIds.contains(componentId)) {
                this.missingComponentIds.add(componentId);
            }
        }
    }
    
    public void addResolvedComponent(ResolvedComponent component) {
        this.resolvedComponents.add(component);
    }
    
    public void addMissingComponent(String componentId) {
        if (!this.missingComponentIds.contains(componentId)) {
            this.missingComponentIds.add(componentId);
        }
    }
    
    public void addCircularReference(String componentId) {
        if (!this.circularReferences.contains(componentId)) {
            this.circularReferences.add(componentId);
        }
    }
    
    public boolean isFullyResolved() {
        return fullyResolved && missingComponentIds.isEmpty();
    }
}
