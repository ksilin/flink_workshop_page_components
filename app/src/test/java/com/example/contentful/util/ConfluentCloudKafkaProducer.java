package com.example.contentful.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Kafka producer utility for sending test data to Confluent Cloud.
 * Handles authentication and provides methods to send Page and Component records.
 */
public class ConfluentCloudKafkaProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ConfluentCloudKafkaProducer.class);

    private final KafkaProducer<String, String> producer;
    private final String pageTopicName;
    private final String componentTopicName;

    public ConfluentCloudKafkaProducer(String bootstrapServers, Map<String, String> authConfig, 
                                     String pageTopicName, String componentTopicName) {
        this.pageTopicName = pageTopicName;
        this.componentTopicName = componentTopicName;
        
        Properties props = new Properties();
        
        // Basic producer configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance and reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);
        
        // Batching for better throughput
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        // Enable idempotent producer for exactly-once semantics
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Add authentication configuration
        authConfig.forEach(props::put);
        
        this.producer = new KafkaProducer<>(props);
        
        logger.info("Initialized Confluent Cloud Kafka producer for topics: '{}' and '{}'", 
            pageTopicName, componentTopicName);
    }

    /**
     * Send test records to appropriate topics based on record type
     */
    public void sendTestRecords(List<TestDataGenerator.TestRecord> records) {
        logger.info("Sending {} test records to Confluent Cloud", records.size());
        
        int pageCount = 0;
        int componentCount = 0;
        
        for (TestDataGenerator.TestRecord record : records) {
            try {
                String topicName = determineTopicName(record);
                
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
                    topicName,
                    record.key,
                    record.jsonValue
                );
                
                // Send asynchronously with callback
                Future<RecordMetadata> future = producer.send(kafkaRecord, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to send record to topic {}: key={}", 
                            topicName, record.key, exception);
                    } else {
                        logger.debug("Successfully sent record to topic {}: key={}, partition={}, offset={}", 
                            metadata.topic(), record.key, metadata.partition(), metadata.offset());
                    }
                });
                
                if ("page".equals(record.recordType)) {
                    pageCount++;
                } else {
                    componentCount++;
                }
                
            } catch (Exception e) {
                logger.error("Failed to send test record: {}", record, e);
            }
        }
        
        // Ensure all records are sent
        producer.flush();
        
        logger.info("Sent {} page records and {} component records to Confluent Cloud", 
            pageCount, componentCount);
    }

    /**
     * Send a single test record
     */
    public Future<RecordMetadata> sendRecord(TestDataGenerator.TestRecord record) {
        String topicName = determineTopicName(record);
        
        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(
            topicName,
            record.key,
            record.jsonValue
        );
        
        logger.debug("Sending single record to topic {}: key={}", topicName, record.key);
        
        return producer.send(kafkaRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send single record to topic {}: key={}", 
                    topicName, record.key, exception);
            } else {
                logger.info("Successfully sent single record to topic {}: key={}, partition={}, offset={}", 
                    metadata.topic(), record.key, metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * Send records in batches for better performance
     */
    public void sendRecordsInBatches(List<TestDataGenerator.TestRecord> records, int batchSize) {
        logger.info("Sending {} records in batches of {}", records.size(), batchSize);
        
        for (int i = 0; i < records.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, records.size());
            List<TestDataGenerator.TestRecord> batch = records.subList(i, endIndex);
            
            logger.info("Sending batch {}-{} of {}", i + 1, endIndex, records.size());
            sendTestRecords(batch);
            
            // Small delay between batches to avoid overwhelming the cluster
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting between batches");
                break;
            }
        }
    }

    private String determineTopicName(TestDataGenerator.TestRecord record) {
        switch (record.recordType) {
            case "page":
                return pageTopicName;
            case "component":
                return componentTopicName;
            default:
                throw new IllegalArgumentException("Unknown record type: " + record.recordType);
        }
    }

    /**
     * Get producer metrics for monitoring
     */
    public Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.Metric> getMetrics() {
        return producer.metrics();
    }

    @Override
    public void close() {
        if (producer != null) {
            logger.info("Closing Kafka producer...");
            producer.flush(); // Ensure all pending records are sent
            producer.close();
            logger.info("Kafka producer closed");
        }
    }

    /**
     * Factory method to create producer with Confluent Cloud configuration
     */
    public static ConfluentCloudKafkaProducer createForConfluentCloud(
            String bootstrapServers, 
            String username, 
            String password,
            String pageTopicName,
            String componentTopicName) {
        
        Map<String, String> authConfig = Map.of(
            "security.protocol", "SASL_SSL",
            "sasl.mechanism", "PLAIN",
            "sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, password
            ),
            "ssl.endpoint.identification.algorithm", "https"
        );
        
        return new ConfluentCloudKafkaProducer(bootstrapServers, authConfig, pageTopicName, componentTopicName);
    }
}