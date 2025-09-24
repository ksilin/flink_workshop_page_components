package com.example.contentful.serde;

import com.example.contentful.model.PageComponent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class PageComponentJsonSerializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(PageComponentJsonSerializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Serialize PageComponent to JSON bytes
     * @param pageComponent The PageComponent to serialize
     * @return JSON bytes
     * @throws IOException if serialization fails
     */
    public byte[] serialize(PageComponent pageComponent) throws IOException {
        if (pageComponent == null) {
            logger.warn("Attempted to serialize null PageComponent");
            return null;
        }

        try {
            byte[] jsonBytes = objectMapper.writeValueAsBytes(pageComponent);
            logger.debug("Serialized PageComponent {} to {} JSON bytes", 
                pageComponent.pageId, jsonBytes.length);
            return jsonBytes;
            
        } catch (Exception e) {
            logger.error("Failed to serialize PageComponent: pageId={}, type={}", 
                pageComponent.pageId, pageComponent.pageType, e);
            throw new IOException("PageComponent JSON serialization failed", e);
        }
    }
}