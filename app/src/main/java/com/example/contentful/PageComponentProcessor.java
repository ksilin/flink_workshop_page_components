package com.example.contentful;

import com.example.contentful.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

// locale is sharding key
public class PageComponentProcessor extends KeyedProcessFunction<String, CommonWrapper, PageComponent> {

    private static final Logger logger = LoggerFactory.getLogger(PageComponentProcessor.class);

    // actual key is locale + entityId, ae have already sharded the data by locale
    // cardinality - 10000 - 100K per locale
    private transient MapState<String, Component> componentStore;

    // dangling references as state store - key: componentId being waited for, value: list of waiting page IDs
    private transient MapState<String, List<String>> waitingPagesStore;

    // page component store - key: pageId, value: PageComponent aggregation
    private transient MapState<String, PageComponent> pageComponentStore;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void processElement(CommonWrapper value, KeyedProcessFunction<String, CommonWrapper, PageComponent>.Context ctx, Collector<PageComponent> out) throws Exception {
        String locale = ctx.getCurrentKey();
        
        logger.debug("Processing {} for locale: {}", value.getType(), locale);

        // differentiate btw component, page, and marker
        switch (value.getType()) {
            case COMPONENT:
                processComponent(value.getComponent(), out);
                break;
            case PAGE:
                processPage(value.getPage(), out);
                break;
            case MARKER:
                logger.debug("Marker received - ignoring");
                break;
            default:
                logger.warn("Unknown wrapper type: {}", value.getType());
        }
    }

    private void processComponent(Component component, Collector<PageComponent> out) throws Exception {
        logger.debug("Processing component: id={}, type={}, published={}, childComponents={}", 
            component.id, component.type, component.published, component.components.size());

        // TODO - perform deletion if published is false

        // store first
        componentStore.put(component.id, component);
        logger.debug("Stored component {} in componentStore", component.id);

        // is this component is being waited for
        List<String> waitingPageIds = waitingPagesStore.get(component.id);
        if (waitingPageIds != null && !waitingPageIds.isEmpty()) {
            logger.debug("Component {} was being waited for by {} pages: {}", 
                component.id, waitingPageIds.size(), waitingPageIds);
            waitingPagesStore.remove(component.id);

            // Process each waiting page
            for (String waitingPageId : waitingPageIds) {
                PageComponent pageComponent = pageComponentStore.get(waitingPageId);
                if (pageComponent != null) {
                    logger.debug("attempting resolution for waiting page: {}", waitingPageId);
                    ResolutionResult result = performPageResolution(pageComponent);
                    emitPageComponentIfReady(pageComponent, result, out);
                } else {
                    logger.warn("Page {} waiting for component {} but not found in pageComponentStore",
                        waitingPageId, component.id);
                }
            }
        }
    }

    private ResolutionResult processPage(Page page, Collector<PageComponent> out) throws Exception {
        logger.debug("Processing page: id={}, type={}, published={}, componentIds={}", 
            page.id, page.type, page.published, page.components);
        
        // TODO - create or UPDATE - we are only creating now
        PageComponent pageComponent = new PageComponent(page.id, page.type, page.locale, page.published, page.components);

        // recursive resolution
        ResolutionResult result = performPageResolution(pageComponent);
        logger.debug("Page {} resolution result: fullyResolved={}, resolvedCount={}, missingCount={}, circularCount={}, depth={}",
            page.id, result.isFullyResolved(), result.getResolvedCount(), result.getMissingCount(), 
            result.getCircularCount(), result.getResolutionDepth());
        
        // store, whether fully resolved or not
        pageComponentStore.put(page.id, pageComponent);
        logger.debug("Stored page {} in pageComponentStore", page.id);
        
        emitPageComponentIfReady(pageComponent, result, out);
        return result;
    }


    private ResolutionResult performPageResolution(PageComponent pageComponent) throws Exception {
        logger.debug("Starting resolution for page {} with {} original component IDs", 
            pageComponent.pageId, pageComponent.pageComponentIds.size());
        
        // TODO - reuse already resolved components
        pageComponent.resolvedComponents.clear();
        pageComponent.missingComponentIds.clear();
        pageComponent.circularReferences.clear();
        
        Set<String> visitedInCurrentPath = new HashSet<>();
        Map<String, PageComponent.ResolvedComponent> resolvedMap = new HashMap<>();
        
        for (String componentId : pageComponent.pageComponentIds) {
            logger.debug("Resolving top-level component {} for page {}", componentId, pageComponent.pageId);
            PageComponent.ResolvedComponent resolved = resolveComponentRecursively(
                componentId, 0, visitedInCurrentPath, resolvedMap, pageComponent
            );
            if (resolved != null) {
                pageComponent.addResolvedComponent(resolved);
                logger.debug("Successfully resolved component {} at depth {}", componentId, resolved.depth);
            } else {
                logger.debug("Component {} could not be resolved (missing or circular)", componentId);
            }
        }
        
        pageComponent.fullyResolved = pageComponent.missingComponentIds.isEmpty();
        pageComponent.resolutionDepth = calculateMaxDepth(pageComponent.resolvedComponents);
        
        return new ResolutionResult(
            pageComponent.isFullyResolved(),
            pageComponent.resolvedComponents.size(),
            pageComponent.missingComponentIds.size(),
            pageComponent.circularReferences.size(),
            pageComponent.resolutionDepth,
            pageComponent.circularReferences,
            pageComponent.missingComponentIds
        );
    }
    
    /**
     * decide whether to produce output
     */
    private void emitPageComponentIfReady(PageComponent pageComponent, ResolutionResult result, Collector<PageComponent> out) throws Exception {
        // if fully resolved
        if (result.isFullyResolved()) {
            logger.debug("Emitting fully resolved page: {} with {} resolved components",
                         pageComponent.pageId, pageComponent.resolvedComponents.size());
            out.collect(pageComponent);
        } else {
            logger.debug("Page {} not ready for output - missing: {}, circular: {}",
                         pageComponent.pageId, result.getMissingComponentIds(), result.getCircularReferences());
        }
    }

    private PageComponent.ResolvedComponent resolveComponentRecursively(
        String componentId, 
        int currentDepth, 
        Set<String> visitedInCurrentPath, 
        Map<String, PageComponent.ResolvedComponent> resolvedMap,
        PageComponent pageComponent
    ) throws Exception {
        
        // Loop detection - just track and stop following the cycle
        if (visitedInCurrentPath.contains(componentId)) {
            logger.debug("Circular reference detected for component {} at depth {} - stopping cycle", componentId, currentDepth);
            pageComponent.addCircularReference(componentId);
            return null; // Stop following the cycle, but this is not a "failure"
        }
        
        // Check if already resolved
        if (resolvedMap.containsKey(componentId)) {
            return resolvedMap.get(componentId);
        }
        
        Component component = componentStore.get(componentId);
        if (component == null) {
            logger.debug("Component {} not found in store - adding to dangling references for page {}", 
                componentId, pageComponent.pageId);
            // Component not found - add to dangling references and page-level missing
            pageComponent.addMissingComponent(componentId);
            addToDanglingReferences(componentId, pageComponent.pageId);
            return null; // Missing component - cannot resolve
        }
        
        // remember for loop detection
        visitedInCurrentPath.add(componentId);
        logger.debug("Resolving component {} at depth {} with {} child components", 
            componentId, currentDepth, component.components.size());

        PageComponent.ResolvedComponent resolved = new PageComponent.ResolvedComponent(component, currentDepth);
        
        // resolve child components
        for (String childComponentId : component.components) {
            PageComponent.ResolvedComponent childResolved = resolveComponentRecursively(
                childComponentId, currentDepth + 1, visitedInCurrentPath, resolvedMap, pageComponent
            );
            if (childResolved != null) {
                resolved.childComponents.add(childResolved);
            } else {
                // Child component could not be resolved (missing or circular) - track it
                resolved.addMissingComponent(childComponentId);
            }
        }
        
        // Remove from current path (backtrack)
        visitedInCurrentPath.remove(componentId);
        
        // Cache the resolved component
        resolvedMap.put(componentId, resolved);
        
        return resolved;
    }

    private void addToDanglingReferences(String componentId, String waitingPageId) throws Exception {
        List<String> waitingPages = waitingPagesStore.get(componentId);
        if (waitingPages == null) {
            waitingPages = new ArrayList<>();
        }
        if (!waitingPages.contains(waitingPageId)) {
            waitingPages.add(waitingPageId);
            waitingPagesStore.put(componentId, waitingPages);
            logger.debug("Added page {} to dangling references for component {} (total waiting: {})", 
                waitingPageId, componentId, waitingPages.size());
        }
    }


    private int calculateMaxDepth(List<PageComponent.ResolvedComponent> components) {
        int maxDepth = 0;
        for (PageComponent.ResolvedComponent component : components) {
            maxDepth = Math.max(maxDepth, calculateComponentDepth(component));
        }
        return maxDepth;
    }

    private int calculateComponentDepth(PageComponent.ResolvedComponent component) {
        int maxChildDepth = component.depth;
        for (PageComponent.ResolvedComponent child : component.childComponents) {
            maxChildDepth = Math.max(maxChildDepth, calculateComponentDepth(child));
        }
        return maxChildDepth;
    }

    // State descriptor fields for test access
    private static final MapStateDescriptor<String, Component> COMPONENT_DESCRIPTOR = new MapStateDescriptor<>(
        "componentStore", 
        TypeInformation.of(String.class), 
        TypeInformation.of(Component.class)
    );
    
    private static final MapStateDescriptor<String, List<String>> DANGLING_DESCRIPTOR = new MapStateDescriptor<>(
        "danglingReferenceStore", 
        TypeInformation.of(String.class), 
        Types.LIST(Types.STRING)
    );
    
    private static final MapStateDescriptor<String, PageComponent> PAGE_COMPONENT_DESCRIPTOR = new MapStateDescriptor<>(
        "pageComponentStore", 
        TypeInformation.of(String.class), 
        TypeInformation.of(PageComponent.class)
    );

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        
        // Initialize state stores using static descriptors
        componentStore = getRuntimeContext().getMapState(COMPONENT_DESCRIPTOR);
        waitingPagesStore = getRuntimeContext().getMapState(DANGLING_DESCRIPTOR);
        pageComponentStore = getRuntimeContext().getMapState(PAGE_COMPONENT_DESCRIPTOR);
    }
    
    // Getters for test access to state descriptors
    public MapStateDescriptor<String, Component> getComponentStateDescriptor() {
        return COMPONENT_DESCRIPTOR;
    }
    
    public MapStateDescriptor<String, List<String>> getDanglingReferenceStateDescriptor() {
        return DANGLING_DESCRIPTOR;
    }
    
    public MapStateDescriptor<String, PageComponent> getPageComponentStateDescriptor() {
        return PAGE_COMPONENT_DESCRIPTOR;
    }
}
