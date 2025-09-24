package com.example.contentful;

import com.example.contentful.model.CommonWrapper;
import com.example.contentful.model.PageComponent;
import com.example.contentful.serde.KafkaRecordToCommonWrapperJsonDeserializer;
import com.example.contentful.serde.PageComponentKafkaRecordSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

class ConfluentCloudPageComponentApp {

    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(2)
                            .setConfiguration(createConfigWithWebUI())
                            .build());

    public static final String bootstrapServerSource = "pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092";
    public static final String bootstrapServerTarget = "pkc-4ygn6.europe-west3.gcp.confluent.cloud:9092";

    public static final String topicPrefix = "ktest-page-component";

    public static final String pageTopic = topicPrefix + "-pages";
    public static final String pageComponentOutputTopic = topicPrefix + "-output-page-components";

    public static final String componentsTopic = topicPrefix + "-components";

    public static final String jobName = "page-components-flink-0.1";

    public static final Logger logger = LoggerFactory.getLogger(ConfluentCloudPageComponentApp.class);

    public static void main(String[] args) throws Exception {

        // Start the MiniCluster
        flinkCluster.before();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String webUIUrl = flinkCluster.getClusterClient().getWebInterfaceURL();
        System.out.println("Flink Web UI available at: " + webUIUrl);

        Map<String, Object> ccConfigSource = Map.of(
                "bootstrap.servers", bootstrapServerSource,
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xx\" password=\"xxx\";",

                "ssl.endpoint.identification.algorithm", "https"
        );

        Map<String, Object> ccConfigSink = Map.of(
                "bootstrap.servers", bootstrapServerSource,
                "security.protocol", "SASL_SSL",
                "sasl.mechanism", "PLAIN",
                "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xx\" password=\"xxx\";",

                "ssl.endpoint.identification.algorithm", "https"
        );

        // JSON deserializer (no Schema Registry needed)
        KafkaRecordToCommonWrapperJsonDeserializer jsonDeserializer =
                new KafkaRecordToCommonWrapperJsonDeserializer(
                        pageTopic,
                        componentsTopic
                );

        // Create Kafka source
        var sourceBuilder = KafkaSource.<CommonWrapper>builder()
                .setBootstrapServers(bootstrapServerSource)
                .setTopics(
                        pageTopic,
                        componentsTopic
                )
                .setStartingOffsets(OffsetsInitializer.timestamp(Instant.now().minus(120, ChronoUnit.HOURS).toEpochMilli()))
                .setBounded(OffsetsInitializer.latest()) // Bounded for test completion
                .setProperty("consumer.session.timeout.ms", "30000")
                .setProperty("consumer.heartbeat.interval.ms", "10000")
                .setDeserializer(jsonDeserializer);

        // Add authentication properties for Confluent Cloud
        for (Map.Entry<String, Object> entry : ccConfigSource.entrySet()) {
            sourceBuilder.setProperty(entry.getKey(), entry.getValue().toString());
        }

        KafkaSource<CommonWrapper> source = sourceBuilder.build();

        // processing pipeline
        DataStream<CommonWrapper> eventWrappers = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        // Create pipeline - union topics, keyBy locale, process
        DataStream<PageComponent> results = PageComponentPipeline.createPipeline(eventWrappers);

        var sinkBuilder = KafkaSink.<PageComponent>builder()
                .setProperty("register.producer.metrics", "true")
                .setBootstrapServers(bootstrapServerTarget)
                .setRecordSerializer(new PageComponentKafkaRecordSerializer(pageComponentOutputTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);

        // Add authentication properties for Confluent Cloud
        for (Map.Entry<String, Object> entry : ccConfigSink.entrySet()) {
            sinkBuilder.setProperty(entry.getKey(), entry.getValue().toString());
        }

        KafkaSink<PageComponent> sink = sinkBuilder.build();

        results.sinkTo(sink);

        logger.info("Executing Flink job: {}", jobName);
        env.execute(jobName);

        // Wait for processing to complete
        Thread.sleep(100000);

        logger.info("Shutting down MiniCluster...");
        flinkCluster.after();
        logger.info("MiniCluster shut down complete");

        // Force exit to prevent hanging
        logger.info("Exiting application...");
        System.exit(0);
    }

    private static Configuration createConfigWithWebUI() {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081);

        // optional - defaults to localhost
        config.setString(RestOptions.BIND_ADDRESS, "localhost");
        config.setString("taskmanager.host", "localhost");

        // Enable web submission and cancellation
        config.setBoolean("web.submit.enable", true);
        config.setBoolean("web.cancel.enable", true);

        // Total process memory - critical for large stateful applications
        // config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("6g"));

        // Managed memory for RocksDB (25% of total Flink memory recommended)
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("4g"));

        // Network memory for high-throughput applications
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("512m"));
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("512m"));

        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");

        // defaults
        config.setString("state.backend.rocksdb.memory.managed", "true");
        config.setString("state.backend.rocksdb.memory.write-buffer-ratio", "0.5");
        config.setString("state.backend.rocksdb.memory.high-prio-pool-ratio", "0.1");

        config.setString("metrics.reporter.jmx.class", "org.apache.flink.metrics.jmx.JMXReporter");
        config.setString("metrics.reporter.jmx.port", "8789");

        // Enable RocksDB native metrics for debugging
        config.setString("state.backend.rocksdb.metrics.actual-delayed-write-rate", "true");
        config.setString("state.backend.rocksdb.metrics.block-cache-usage", "true");
        config.setString("state.backend.rocksdb.metrics.mem-table-flush-pending", "true");

        // defaults to 30s - increasing to 60s might affect deadlocks
        config.setString("taskmanager.network.memory.exclusive-buffers-request-timeout-ms", "60000");

        config.setString("taskmanager.debug.memory.log", "true");
        config.setString("taskmanager.debug.memory.log-interval", "5000");

        config.setString("taskmanager.network.request-backoff.initial", "100");
        config.setString("taskmanager.network.request-backoff.max", "10000");

        // in case there are multiple attached disks, select the most local one
        config.setString("state.backend.rocksdb.localdir", "/tmp/rocksdb-temp");

        // Timeout configurations
        config.setString("pekko.ask.timeout", "60s");
        config.setString("web.timeout", "60000");

        config.setString("rest.flamegraph.enabled", "true");
        config.setString("metrics.latency.interval", "5000");
        config.setString("web.backpressure.enabled", "true");
        config.setString("web.backpressure.cleanup-interval", "600000");
        config.setString("web.backpressure.refresh-interval", "5000");

        // Task slots - adjust based on your parallelism requirements
        config.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);

        return config;
    }

}