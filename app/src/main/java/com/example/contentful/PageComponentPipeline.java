package com.example.contentful;

import com.example.contentful.model.CommonWrapper;
import com.example.contentful.model.PageComponent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageComponentPipeline {

    private static final Logger logger = LoggerFactory.getLogger(PageComponentPipeline.class);

    /**
     * @param pageStream Stream containing Page events
     * @param componentStream Stream containing Component events
     * @return Stream of resolved PageComponent objects
     */
    public static DataStream<PageComponent> createPipeline(
            DataStream<CommonWrapper> pageStream,
            DataStream<CommonWrapper> componentStream) {

        DataStream<CommonWrapper> unifiedStream = pageStream.union(componentStream);

        DataStream<CommonWrapper> processedUnifiedStream = unifiedStream;

        // Key by locale and process with PageComponentProcessor
        return processedUnifiedStream
            .keyBy(CommonWrapper::extractLocale)
            .process(new PageComponentProcessor());
    }

    /**
     * Create pipeline from a single mixed CommonWrapper stream
     *
     * @param inputStream Stream containing both Page and Component events
     * @param enableDebugLogging Whether to add debug logging
     * @return Stream of resolved PageComponent objects
     */
    public static DataStream<PageComponent> createPipeline(
            DataStream<CommonWrapper> inputStream,
            boolean enableDebugLogging) {

        logger.info("Creating PageComponent pipeline from unified input stream with debug logging: {}", enableDebugLogging);

        // Optional debug logging
        DataStream<CommonWrapper> loggedStream = inputStream;
        if (enableDebugLogging) {
            loggedStream = inputStream.map(wrapper -> {
                logger.info("Pipeline input: type={}, locale={}", 
                    wrapper.getType(), wrapper.extractLocale());
                if (wrapper.getType() == CommonWrapper.EntityType.PAGE) {
                    logger.info("  Page: {} with {} component references", 
                        wrapper.getPage().id, wrapper.getPage().components.size());
                } else if (wrapper.getType() == CommonWrapper.EntityType.COMPONENT) {
                    logger.info("  Component: {} with {} child components", 
                        wrapper.getComponent().id, wrapper.getComponent().components.size());
                }
                return wrapper;
            });
        }

        // Key by locale and process with PageComponentProcessor
        return loggedStream
            .keyBy(CommonWrapper::extractLocale)
            .process(new PageComponentProcessor());
    }

    /**
     * Create pipeline with debug logging enabled (useful for tests)
     */
    public static DataStream<PageComponent> createPipeline(DataStream<CommonWrapper> inputStream) {
        return createPipeline(inputStream, true);
    }

    /**
     * Create pipeline for production use (no debug logging)  
     */
    public static DataStream<PageComponent> createPipelineForProduction(DataStream<CommonWrapper> inputStream) {
        return createPipeline(inputStream, false);
    }
}