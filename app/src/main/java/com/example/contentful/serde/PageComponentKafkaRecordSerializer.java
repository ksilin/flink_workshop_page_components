package com.example.contentful.serde;

import com.example.contentful.model.PageComponent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key format: "pageId-locale" (e.g., "page1-en-US")
 * Value: JSON serialized PageComponent
 */
public class PageComponentKafkaRecordSerializer implements KafkaRecordSerializationSchema<PageComponent> {

    private static final Logger logger = LoggerFactory.getLogger(PageComponentKafkaRecordSerializer.class);

    private final String topic;
    private final PageComponentJsonSerializer valueSerializer;

    public PageComponentKafkaRecordSerializer(String topic) {
        this.topic = topic;
        this.valueSerializer = new PageComponentJsonSerializer();
        logger.info("Initialized PageComponent Kafka serializer for topic: '{}'", topic);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(PageComponent pageComponent, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] value = valueSerializer.serialize(pageComponent);

            String recordKey = getRecordKey(pageComponent);
            byte[] key = null;

            if (recordKey != null) {
                key = recordKey.getBytes("UTF-8");
                logger.debug("Serializing PageComponent with key: '{}' for topic: '{}'", recordKey, topic);
            } else {
                logger.warn("PageComponent has null key - message will not be properly compacted. PageComponent: {}",
                    pageComponent != null ? pageComponent.pageId : "null");
            }

            return new ProducerRecord<>(topic, null, timestamp, key, value);

        } catch (Exception e) {
            logger.error("Failed to serialize PageComponent for topic: {}", topic, e);
            throw new RuntimeException("PageComponent serialization failed", e);
        }
    }

    /**
     * Generate Kafka record key in format: "pageId-locale"
     * @param pageComponent The PageComponent to extract key from
     * @return Record key string, or null if pageComponent or required fields are null
     */
    public String getRecordKey(PageComponent pageComponent) {
        if (pageComponent != null && pageComponent.pageId != null) {
            // Extract locale from the first component or use a default
            String locale = extractLocale(pageComponent);
            return pageComponent.pageId + "-" + locale;
        }
        return null;
    }

    private String extractLocale(PageComponent pageComponent) {
        if (pageComponent.locale != null && !pageComponent.locale.isEmpty()) {
            return pageComponent.locale;
        }
        logger.warn("PageComponent {} has null/empty locale, using default 'en-US'", pageComponent.pageId);
        return "en-US";
    }
}