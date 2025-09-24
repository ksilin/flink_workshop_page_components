package com.example.contentful.model;

import java.util.List;

public class Page {

    	public String id;
    	public String type;
    	public Boolean published;
    	public List<String> components;
    	public String locale;


    public Page(String id, String type, Boolean published, List<String> components, String locale) {
        this.id = id;
        this.type = type;
        this.published = published;
        this.components = components;
        this.locale = locale;
    }
}
