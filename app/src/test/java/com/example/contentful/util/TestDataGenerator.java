package com.example.contentful.util;

import com.example.contentful.model.Component;
import com.example.contentful.model.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Generates test data for Page and Component records.
 */
public class TestDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(TestDataGenerator.class);
    private final ObjectMapper objectMapper;

    public TestDataGenerator() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Generate test pages for different locales and scenarios
     */
    public List<TestRecord> generateTestPages() {
        List<TestRecord> records = new ArrayList<>();

        // Landing pages for different locales
        records.add(createPageRecord("landing-1", "landing", "en-US", true, 
            Arrays.asList("header-en", "hero-banner-en", "footer-en")));
        
        records.add(createPageRecord("landing-1", "landing", "de-DE", true, 
            Arrays.asList("header-de", "hero-banner-de", "footer-de")));

        records.add(createPageRecord("landing-1", "landing", "fr-FR", true, 
            Arrays.asList("header-fr", "hero-banner-fr", "footer-fr")));

        // Product pages with more complex component structure
        records.add(createPageRecord("product-page-1", "product", "en-US", true, 
            Arrays.asList("header-en", "product-gallery-en", "product-info-en", "reviews-section-en", "footer-en")));

        records.add(createPageRecord("product-page-1", "product", "de-DE", true, 
            Arrays.asList("header-de", "product-gallery-de", "product-info-de", "reviews-section-de", "footer-de")));

        // Blog page
        records.add(createPageRecord("blog-post-1", "blog", "en-US", true, 
            Arrays.asList("header-en", "article-content-en", "author-bio-en", "comments-section-en", "footer-en")));

        // Unpublished page (should still be processed but marked as unpublished)
        records.add(createPageRecord("draft-page-1", "landing", "en-US", false, 
            Arrays.asList("header-en", "draft-content-en")));

        logger.info("Generated {} test page records", records.size());
        return records;
    }

    /**
     * Generate test components for different locales and types
     */
    public List<TestRecord> generateTestComponents() {
        List<TestRecord> records = new ArrayList<>();

        // English components
        records.addAll(generateComponentsForLocale("en-US"));
        
        // German components
        records.addAll(generateComponentsForLocale("de-DE"));
        
        // French components
        records.addAll(generateComponentsForLocale("fr-FR"));

        logger.info("Generated {} test component records", records.size());
        return records;
    }

    private List<TestRecord> generateComponentsForLocale(String locale) {
        List<TestRecord> components = new ArrayList<>();
        String suffix = locale.equals("en-US") ? "en" : (locale.equals("de-DE") ? "de" : "fr");

        // Header component (contains navigation and logo)
        components.add(createComponentRecord("header-" + suffix, "header", locale, true, 
            Arrays.asList("logo-" + suffix, "navigation-" + suffix)));

        // Simple leaf components for header
        components.add(createComponentRecord("logo-" + suffix, "image", locale, true, List.of()));
        components.add(createComponentRecord("navigation-" + suffix, "menu", locale, true, 
            Arrays.asList("nav-home-" + suffix, "nav-products-" + suffix, "nav-about-" + suffix)));
        
        // Navigation items
        components.add(createComponentRecord("nav-home-" + suffix, "link", locale, true, List.of()));
        components.add(createComponentRecord("nav-products-" + suffix, "link", locale, true, List.of()));
        components.add(createComponentRecord("nav-about-" + suffix, "link", locale, true, List.of()));

        // Hero banner component (complex nested structure)
        components.add(createComponentRecord("hero-banner-" + suffix, "hero", locale, true, 
            Arrays.asList("hero-title-" + suffix, "hero-subtitle-" + suffix, "hero-cta-" + suffix, "hero-image-" + suffix)));

        components.add(createComponentRecord("hero-title-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("hero-subtitle-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("hero-cta-" + suffix, "button", locale, true, List.of()));
        components.add(createComponentRecord("hero-image-" + suffix, "image", locale, true, List.of()));

        // Product-specific components
        components.add(createComponentRecord("product-gallery-" + suffix, "gallery", locale, true, 
            Arrays.asList("product-image-1-" + suffix, "product-image-2-" + suffix, "product-image-3-" + suffix)));
        
        components.add(createComponentRecord("product-image-1-" + suffix, "image", locale, true, List.of()));
        components.add(createComponentRecord("product-image-2-" + suffix, "image", locale, true, List.of()));
        components.add(createComponentRecord("product-image-3-" + suffix, "image", locale, true, List.of()));

        components.add(createComponentRecord("product-info-" + suffix, "info-panel", locale, true, 
            Arrays.asList("product-title-" + suffix, "product-price-" + suffix, "product-description-" + suffix, "add-to-cart-" + suffix)));
        
        components.add(createComponentRecord("product-title-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("product-price-" + suffix, "price", locale, true, List.of()));
        components.add(createComponentRecord("product-description-" + suffix, "rich-text", locale, true, List.of()));
        components.add(createComponentRecord("add-to-cart-" + suffix, "button", locale, true, List.of()));

        // Reviews section
        components.add(createComponentRecord("reviews-section-" + suffix, "reviews", locale, true, 
            Arrays.asList("reviews-title-" + suffix, "review-list-" + suffix, "add-review-" + suffix)));
        
        components.add(createComponentRecord("reviews-title-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("review-list-" + suffix, "list", locale, true, List.of()));
        components.add(createComponentRecord("add-review-" + suffix, "form", locale, true, List.of()));

        // Blog-specific components
        components.add(createComponentRecord("article-content-" + suffix, "article", locale, true, 
            Arrays.asList("article-title-" + suffix, "article-body-" + suffix, "article-tags-" + suffix)));
        
        components.add(createComponentRecord("article-title-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("article-body-" + suffix, "rich-text", locale, true, List.of()));
        components.add(createComponentRecord("article-tags-" + suffix, "tag-list", locale, true, List.of()));

        components.add(createComponentRecord("author-bio-" + suffix, "author-card", locale, true, 
            Arrays.asList("author-name-" + suffix, "author-avatar-" + suffix, "author-description-" + suffix)));
        
        components.add(createComponentRecord("author-name-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("author-avatar-" + suffix, "image", locale, true, List.of()));
        components.add(createComponentRecord("author-description-" + suffix, "text", locale, true, List.of()));

        components.add(createComponentRecord("comments-section-" + suffix, "comments", locale, true, 
            Arrays.asList("comments-title-" + suffix, "comment-form-" + suffix)));
        
        components.add(createComponentRecord("comments-title-" + suffix, "text", locale, true, List.of()));
        components.add(createComponentRecord("comment-form-" + suffix, "form", locale, true, List.of()));

        // Footer component
        components.add(createComponentRecord("footer-" + suffix, "footer", locale, true, 
            Arrays.asList("footer-links-" + suffix, "footer-social-" + suffix, "footer-copyright-" + suffix)));
        
        components.add(createComponentRecord("footer-links-" + suffix, "link-list", locale, true, List.of()));
        components.add(createComponentRecord("footer-social-" + suffix, "social-links", locale, true, List.of()));
        components.add(createComponentRecord("footer-copyright-" + suffix, "text", locale, true, List.of()));

        // Draft content (unpublished)
        components.add(createComponentRecord("draft-content-" + suffix, "content-block", locale, false, List.of()));

        return components;
    }

    private TestRecord createPageRecord(String id, String type, String locale, boolean published, List<String> componentIds) {
        Page page = new Page(id, type, published, componentIds, locale);
        try {
            String json = objectMapper.writeValueAsString(page);
            return new TestRecord("page", id + "-" + locale, json, locale);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize page: " + id, e);
        }
    }

    private TestRecord createComponentRecord(String id, String type, String locale, boolean published, List<String> childComponents) {
        Component component = new Component(id, type, id + "-label", id + "-name", published, childComponents, locale);
        try {
            String json = objectMapper.writeValueAsString(component);
            return new TestRecord("component", id, json, locale);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize component: " + id, e);
        }
    }

    public static class TestRecord {
        public final String recordType; // "page" or "component"
        public final String key;
        public final String jsonValue;
        public final String locale;

        public TestRecord(String recordType, String key, String jsonValue, String locale) {
            this.recordType = recordType;
            this.key = key;
            this.jsonValue = jsonValue;
            this.locale = locale;
        }

        @Override
        public String toString() {
            return String.format("TestRecord{type=%s, key=%s, locale=%s, json=%s}", 
                recordType, key, locale, jsonValue.substring(0, Math.min(50, jsonValue.length())) + "...");
        }
    }
}