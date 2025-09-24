package com.example.contentful.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Note: This test only runs when CONFLUENT_CLOUD_TEST=true environment variable is set
 */
public class ConfluentCloudDataGeneratorTest {

    private static final Logger logger = LoggerFactory.getLogger(ConfluentCloudDataGeneratorTest.class);

    // Confluent Cloud configuration (same as ConfluentCloudPageComponentApp)
    private static final String BOOTSTRAP_SERVERS = "pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092";
    private static final String USERNAME = "xx";
    private static final String PASSWORD = "xxx";

    // Topic configuration
    private static final String TOPIC_PREFIX = "ktest-page-component";
    private static final String PAGE_TOPIC = TOPIC_PREFIX + "-pages";
    private static final String COMPONENT_TOPIC = TOPIC_PREFIX + "-components";

    private TestDataGenerator dataGenerator;

    @BeforeEach
    public void setup() {
        dataGenerator = new TestDataGenerator();
        logger.info("Starting Confluent Cloud data generation test");
        logger.info("Target topics: '{}' and '{}'", PAGE_TOPIC, COMPONENT_TOPIC);
    }

    @Test
    @Disabled
    public void testSendTestDataToConfluentCloud() throws Exception {
        logger.info("=== GENERATING TEST DATA ===");

        // Generate test data
        List<TestDataGenerator.TestRecord> pageRecords = dataGenerator.generateTestPages();
        List<TestDataGenerator.TestRecord> componentRecords = dataGenerator.generateTestComponents();

        logger.info("Generated {} page records and {} component records", 
            pageRecords.size(), componentRecords.size());

        // Combine all records
        List<TestDataGenerator.TestRecord> allRecords = new java.util.ArrayList<>(componentRecords);
        allRecords.addAll(pageRecords); // Send pages after components for better resolution

        // Create producer and send data
        try (ConfluentCloudKafkaProducer producer = ConfluentCloudKafkaProducer.createForConfluentCloud(
                BOOTSTRAP_SERVERS, USERNAME, PASSWORD, PAGE_TOPIC, COMPONENT_TOPIC)) {

            logger.info("=== SENDING COMPONENT RECORDS FIRST ===");
            // Send components first so they're available when pages are processed
            producer.sendTestRecords(componentRecords);

            // Wait a bit between batches
            logger.info("Waiting 2 seconds before sending pages...");
            Thread.sleep(2000);

            logger.info("=== SENDING PAGE RECORDS ===");
            // Send pages after components
            producer.sendTestRecords(pageRecords);

            logger.info("=== DATA GENERATION COMPLETE ===");
            logger.info("Sent {} total records to Confluent Cloud", allRecords.size());

            // Log sample data for verification
            logSampleRecords(pageRecords, componentRecords);

        } catch (Exception e) {
            logger.error("Failed to send test data to Confluent Cloud", e);
            fail("Data generation failed: " + e.getMessage());
        }
    }

    @Test
    @Disabled
    public void testSendTestDataInBatches() throws Exception {
        logger.info("=== TESTING BATCH DATA SENDING ===");

        // Generate a smaller dataset for batch testing
        List<TestDataGenerator.TestRecord> pageRecords = dataGenerator.generateTestPages();
        List<TestDataGenerator.TestRecord> componentRecords = dataGenerator.generateTestComponents();

        // Take first 20 components and 5 pages for batch test
        List<TestDataGenerator.TestRecord> testComponents = componentRecords.subList(0, Math.min(20, componentRecords.size()));
        List<TestDataGenerator.TestRecord> testPages = pageRecords.subList(0, Math.min(5, pageRecords.size()));

        try (ConfluentCloudKafkaProducer producer = ConfluentCloudKafkaProducer.createForConfluentCloud(
                BOOTSTRAP_SERVERS, USERNAME, PASSWORD, PAGE_TOPIC, COMPONENT_TOPIC)) {

            // Send in small batches
            logger.info("Sending {} components in batches of 5", testComponents.size());
            producer.sendRecordsInBatches(testComponents, 5);

            Thread.sleep(1000);

            logger.info("Sending {} pages in batches of 2", testPages.size());
            producer.sendRecordsInBatches(testPages, 2);

            logger.info("Batch testing complete");

        } catch (Exception e) {
            logger.error("Batch test failed", e);
            fail("Batch data generation failed: " + e.getMessage());
        }
    }

    @Test
    @Disabled
    public void testGenerateMinimalTestData() throws Exception {
        logger.info("=== GENERATING MINIMAL TEST DATA SET ===");

        // Create a minimal test scenario for quick testing
        try (ConfluentCloudKafkaProducer producer = ConfluentCloudKafkaProducer.createForConfluentCloud(
                BOOTSTRAP_SERVERS, USERNAME, PASSWORD, PAGE_TOPIC, COMPONENT_TOPIC)) {

            // Send minimal components for a simple page
            TestDataGenerator.TestRecord headerComp = createSimpleComponent("test-header", "header", "en-US");
            TestDataGenerator.TestRecord contentComp = createSimpleComponent("test-content", "content", "en-US");
            TestDataGenerator.TestRecord footerComp = createSimpleComponent("test-footer", "footer", "en-US");

            producer.sendRecord(headerComp).get(10, TimeUnit.SECONDS);
            producer.sendRecord(contentComp).get(10, TimeUnit.SECONDS);
            producer.sendRecord(footerComp).get(10, TimeUnit.SECONDS);

            // Send a simple page that references these components
            TestDataGenerator.TestRecord simplePage = createSimplePage("test-simple-page", "landing", "en-US",
                List.of("test-header", "test-content", "test-footer"));

            producer.sendRecord(simplePage).get(10, TimeUnit.SECONDS);

            logger.info("Sent minimal test data: 3 components + 1 page");

        } catch (Exception e) {
            logger.error("Minimal test data generation failed", e);
            fail("Minimal data generation failed: " + e.getMessage());
        }
    }

    private TestDataGenerator.TestRecord createSimpleComponent(String id, String type, String locale) {
        com.example.contentful.model.Component component = new com.example.contentful.model.Component(
            id, type, id + "-label", id + "-name", true, List.of(), locale);
        
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            String json = mapper.writeValueAsString(component);
            return new TestDataGenerator.TestRecord("component", id, json, locale);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test component", e);
        }
    }

    private TestDataGenerator.TestRecord createSimplePage(String id, String type, String locale, List<String> componentIds) {
        com.example.contentful.model.Page page = new com.example.contentful.model.Page(
            id, type, true, componentIds, locale);
        
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            String json = mapper.writeValueAsString(page);
            return new TestDataGenerator.TestRecord("page", id + "-" + locale, json, locale);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test page", e);
        }
    }

    private void logSampleRecords(List<TestDataGenerator.TestRecord> pageRecords, 
                                List<TestDataGenerator.TestRecord> componentRecords) {
        logger.info("=== SAMPLE DATA VERIFICATION ===");
        
        logger.info("Sample Page Record:");
        if (!pageRecords.isEmpty()) {
            TestDataGenerator.TestRecord samplePage = pageRecords.get(0);
            logger.info("  Topic: {}", PAGE_TOPIC);
            logger.info("  Key: {}", samplePage.key);
            logger.info("  Locale: {}", samplePage.locale);
            logger.info("  JSON: {}", samplePage.jsonValue.substring(0, Math.min(200, samplePage.jsonValue.length())));
        }

        logger.info("Sample Component Record:");
        if (!componentRecords.isEmpty()) {
            TestDataGenerator.TestRecord sampleComponent = componentRecords.get(0);
            logger.info("  Topic: {}", COMPONENT_TOPIC);
            logger.info("  Key: {}", sampleComponent.key);
            logger.info("  Locale: {}", sampleComponent.locale);
            logger.info("  JSON: {}", sampleComponent.jsonValue.substring(0, Math.min(200, sampleComponent.jsonValue.length())));
        }

        logger.info("=== VERIFICATION COMPLETE ===");
        logger.info("You can now run the ConfluentCloudPageComponentApp to process this data!");
    }

    /**
     * validate that test data generation works
     */
    @Test
    @Disabled
    public void testDataGenerationWithoutSending() {

        List<TestDataGenerator.TestRecord> pageRecords = dataGenerator.generateTestPages();
        List<TestDataGenerator.TestRecord> componentRecords = dataGenerator.generateTestComponents();

        // Validate generated data
        assertFalse(pageRecords.isEmpty(), "Should generate page records");
        assertFalse(componentRecords.isEmpty(), "Should generate component records");

        // Check that records have required fields
        pageRecords.forEach(record -> {
            assertNotNull(record.key, "Page record should have key");
            assertNotNull(record.jsonValue, "Page record should have JSON value");
            assertNotNull(record.locale, "Page record should have locale");
            assertEquals("page", record.recordType, "Page record should have correct type");
        });

        componentRecords.forEach(record -> {
            assertNotNull(record.key, "Component record should have key");
            assertNotNull(record.jsonValue, "Component record should have JSON value");  
            assertNotNull(record.locale, "Component record should have locale");
            assertEquals("component", record.recordType, "Component record should have correct type");
        });

        logger.info("Data generation validation passed: {} pages, {} components", 
            pageRecords.size(), componentRecords.size());
    }
}