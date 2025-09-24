package com.example.contentful.model;

import java.util.List;

public class Component {

    	public String id;
    	public String type;
    	public String label;
    	public String name;
        public Boolean published;
    	public List<String> components;
    	public String locale;

    // Default constructor for POJO & Jackson
    public Component() {
    }

    public Component(String id, String type, String label, String name, Boolean published, List<String> components, String locale) {
        this.id = id;
        this.label = label;
        this.name = name;
        this.type = type;
        this.published = published;
        this.components = components;
        this.locale = locale;
    }
}
