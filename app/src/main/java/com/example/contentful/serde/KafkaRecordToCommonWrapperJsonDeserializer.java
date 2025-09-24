package com.example.contentful.serde;

import com.example.contentful.model.CommonWrapper;
import com.example.contentful.model.Component;
import com.example.contentful.model.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * JSON deserializer for Page and Component Kafka records into CommonWrapper instances.
 * 
 * Topic determination is based on topic name patterns.
 */
public class KafkaRecordToCommonWrapperJsonDeserializer implements KafkaRecordDeserializationSchema<CommonWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordToCommonWrapperJsonDeserializer.class);

    private final String pageTopicName;
    private final String componentTopicName;
    private final ObjectMapper objectMapper;

    /**
     * Constructor with configurable topic names
     * @param pageTopicName Name of the page topic (e.g., "pages")
     * @param componentTopicName Name of the component topic (e.g., "components")
     */
    public KafkaRecordToCommonWrapperJsonDeserializer(String pageTopicName, String componentTopicName) {
        this.pageTopicName = pageTopicName;
        this.componentTopicName = componentTopicName;
        this.objectMapper = new ObjectMapper();
        
        logger.info("Initialized JSON deserializer for page topic '{}' and component topic '{}'", 
            pageTopicName, componentTopicName);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<CommonWrapper> out) throws IOException {
        if (record.value() == null) {
            logger.warn("Received tombstone from topic: {} - skipping", record.topic());
            return;
        }

        String topic = record.topic();
        String jsonPayload = new String(record.value());
        
        logger.debug("Deserializing message from topic: {} with payload length: {}", topic, jsonPayload.length());

        try {
            CommonWrapper wrapper;
            
            if (topic.equals(pageTopicName)) {
                // Deserialize as Page
                Page page = objectMapper.readValue(jsonPayload, Page.class);
                wrapper = new CommonWrapper(page);
                logger.debug("Deserialized Page with ID: {} and locale: {} from topic: {}", 
                    page.id, page.locale, topic);
                
            } else if (topic.equals(componentTopicName)) {
                // Deserialize as Component
                Component component = objectMapper.readValue(jsonPayload, Component.class);
                wrapper = new CommonWrapper(component);
                logger.debug("Deserialized Component with ID: {} and locale: {} from topic: {}", 
                    component.id, component.locale, topic);
                
            } else {
                logger.warn("Unknown topic: {} - expected '{}' or '{}'. Skipping message.", 
                    topic, pageTopicName, componentTopicName);
                return;
            }
            
            out.collect(wrapper);
            
        } catch (Exception e) {
            logger.error("Failed to deserialize JSON message from topic: {}. Payload: {}", 
                topic, jsonPayload.substring(0, Math.min(200, jsonPayload.length())), e);
            throw new IOException("JSON deserialization failed for topic: " + topic, e);
        }
    }

    @Override
    public TypeInformation<CommonWrapper> getProducedType() {
        return TypeInformation.of(CommonWrapper.class);
    }
}