package com.example.contentful.model;

public class CommonWrapper {
    public enum EntityType {
        PAGE, COMPONENT, MARKER
    }

    public EntityType type;
    public String batchId;

    // only one will be non-null
    public Page page;
    public Component component;
    public Marker marker;

    // Default ctor for POJO
    public CommonWrapper() {}

    public CommonWrapper(Page page) {
        this.type = EntityType.PAGE;
        this.page = page;
    }

    public CommonWrapper(Component component) {
        this.type = EntityType.COMPONENT;
        this.component = component;
    }


    public CommonWrapper(Marker marker) {
        this.type = EntityType.MARKER;
        this.marker = marker;
    }

    public EntityType getType() {
        return type;
    }

    public void setType(EntityType type) {
        this.type = type;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public Page getPage() {
        return page;
    }

    public void setPage(Page page) {
        this.page = page;
    }

    public Component getComponent() {
        return component;
    }

    public void setComponent(Component component) {
        this.component = component;
    }

    public Marker getMarker() {
        return marker;
    }

    public void setMarker(Marker marker) {
        this.marker = marker;
    }

    public String extractLocale() {
        switch (type) {
            case PAGE:
                return page.locale;
            case COMPONENT:
                return component.locale;
            case MARKER:
                return "default-locale"; // Markers don't have locale
            default:
                return "unknown-locale";
        }
    }
}