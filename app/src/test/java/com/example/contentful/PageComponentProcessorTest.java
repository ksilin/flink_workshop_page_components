package com.example.contentful;

import com.example.contentful.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PageComponentProcessorTest {

    private KeyedOneInputStreamOperatorTestHarness<String, CommonWrapper, PageComponent> testHarness;
    private PageComponentProcessor processor;

    @BeforeEach
    public void setup() throws Exception {
        processor = new PageComponentProcessor();
        // key by locale
        testHarness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                processor,
                CommonWrapper::extractLocale, // key selector
                TypeInformation.of(String.class)  // key type
        );
        testHarness.open();
    }

    @Test
    public void testPagesOnly_ShouldStoreWithoutOutput() throws Exception {
        // Send only pages
        Page page1 = new Page("page1", "landing", true, Arrays.asList("comp1", "comp2"), "en-US");
        Page page2 = new Page("page2", "blog", true, List.of("comp3"), "en-US");

        testHarness.processElement(new CommonWrapper(page1), 1000L);
        testHarness.processElement(new CommonWrapper(page2), 2000L);

        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(0, outputs.size(), "Should produce no output when components are missing");

        // TODO - state introspection
    }

    @Test
    public void testComponentsOnly_ShouldStoreWithoutOutput() throws Exception {

        Component comp1 = createComponent("comp1", "text", List.of());
        Component comp2 = createComponent("comp2", "container", List.of("comp3"));
        Component comp3 = createComponent("comp3", "image", List.of());

        testHarness.processElement(new CommonWrapper(comp1), 1000L);
        testHarness.processElement(new CommonWrapper(comp2), 1001L);
        testHarness.processElement(new CommonWrapper(comp3), 1002L);

        // state introspection
        MapState<String, Component> componentState =
                testHarness.getOperator().getKeyedStateStore().getMapState(processor.getComponentStateDescriptor());

        // components are stored in state
        Component storedComp1 = componentState.get("comp1");
        assertNotNull(storedComp1, "Component comp1 should be stored in state");
        assertEquals("comp1", storedComp1.id);

        Component storedComp2 = componentState.get("comp2");
        assertNotNull(storedComp2, "Component comp2 should be stored in state");
        assertEquals("comp2", storedComp2.id);

        Component storedComp3 = componentState.get("comp3");
        assertNotNull(storedComp3, "Component comp3 should be stored in state");
        assertEquals("comp3", storedComp3.id);

        // no pages -> no output
        assertTrue(testHarness.getOutput().isEmpty(), "No output without pages");
    }

    @Test
    public void testPagesThenComponents_ShouldResolveWhenComponentsArrive() throws Exception {

        // First send page (creates dangling references), then send components (triggers resolution)
        Page page = new Page("page1", "landing", true, List.of("comp1"), "en-US");
        Component comp1 = createComponent("comp1", "text", List.of());

        // Send page first - no output expected
        testHarness.processElement(new CommonWrapper(page), 1000L);
        assertEquals(0, testHarness.extractOutputValues().size(), "No output before components arrive");

        // Send component - should trigger resolution and produce output
        testHarness.processElement(new CommonWrapper(comp1), 2000L);
        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size(), "Should produce output when all components resolved");

        // Verify output contains expected page data
        verifyPageComponentOutput(outputs.get(0), "page1", "landing", 1);
    }

    @Test
    public void testMissingComponents_ShouldNotProduceOutput() throws Exception {
        // Components that never arrive
        Page page = new Page("page1", "landing", true, Arrays.asList("comp1", "missing-comp"), "en-US");
        Component comp1 = createComponent("comp1", "text", List.of());

        testHarness.processElement(new CommonWrapper(page), 1000L);
        testHarness.processElement(new CommonWrapper(comp1), 2000L);

        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(0, outputs.size(), "Should not produce output when some components are missing");
    }

    @Test
    public void testComponentsThenPages_ShouldResolveImmediately() throws Exception {
        // Send components first, then pages - should resolve immediately
        // Tests both flat structure and wide structure (multiple direct children)
        Component comp1 = createComponent("comp1", "text", List.of());
        Component comp2 = createComponent("comp2", "image", List.of());
        Component comp3 = createComponent("comp3", "button", List.of());
        Page page = new Page("page1", "landing", true, Arrays.asList("comp1", "comp2", "comp3"), "en-US");

        // Send components first
        testHarness.processElement(new CommonWrapper(comp1), 1000L);
        testHarness.processElement(new CommonWrapper(comp2), 2000L);
        testHarness.processElement(new CommonWrapper(comp3), 3000L);
        assertEquals(0, testHarness.extractOutputValues().size(), "No output before pages arrive");

        // Send page - should resolve immediately with all 3 components
        testHarness.processElement(new CommonWrapper(page), 4000L);
        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size(), "Should produce output immediately when components available");

        verifyPageComponentOutput(outputs.get(0), "page1", "landing", 3);
    }


    @Test
    public void testNestedStructure_ComponentChain() throws Exception {
        // Deep chain: Page -> Comp1 -> Comp2 -> Comp3
        Component comp3 = createComponent("comp3", "text", List.of()); // leaf
        Component comp2 = createComponent("comp2", "container", List.of("comp3"));
        Component comp1 = createComponent("comp1", "container", List.of("comp2"));
        Page page = new Page("page1", "landing", true, List.of("comp1"), "en-US");

        // Send in reverse order to test dangling reference resolution
        testHarness.processElement(new CommonWrapper(page), 1000L);
        testHarness.processElement(new CommonWrapper(comp1), 2000L);
        testHarness.processElement(new CommonWrapper(comp2), 3000L);
        testHarness.processElement(new CommonWrapper(comp3), 4000L);

        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size(), "Should resolve deep nested structure");

        // Verify nested structure in output
        PageComponent result = outputs.get(0);
        assertEquals(1, result.resolvedComponents.size());
        PageComponent.ResolvedComponent root = result.resolvedComponents.get(0);
        assertEquals("comp1", root.id);
        assertEquals(1, root.childComponents.size());
        assertEquals("comp2", root.childComponents.get(0).id);
        assertEquals(1, root.childComponents.get(0).childComponents.size());
        assertEquals("comp3", root.childComponents.get(0).childComponents.get(0).id);
    }

    @Test
    public void testMixedStructure_TreeLikeStructure() throws Exception {
        // Mixed tree: Page -> Comp1 -> [Comp2, Comp3]
        Component comp2 = createComponent("comp2", "text", List.of());
        Component comp3 = createComponent("comp3", "image", List.of());
        Component comp1 = createComponent("comp1", "container", Arrays.asList("comp2", "comp3"));
        Page page = new Page("page1", "landing", true, List.of("comp1"), "en-US");

        testHarness.processElement(new CommonWrapper(comp2), 1000L);
        testHarness.processElement(new CommonWrapper(comp3), 2000L);
        testHarness.processElement(new CommonWrapper(comp1), 3000L);
        testHarness.processElement(new CommonWrapper(page), 4000L);

        List<PageComponent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size(), "Should resolve mixed tree structure");

        // Verify tree structure
        PageComponent result = outputs.get(0);
        PageComponent.ResolvedComponent root = result.resolvedComponents.get(0);
        assertEquals("comp1", root.id);
        assertEquals(2, root.childComponents.size());
    }

    @Test
    public void testStateIntrospection_DanglingReferences() throws Exception {

        // page references non-existent components
        Page page = new Page("page1", "landing", true, Arrays.asList("comp1", "comp2", "missing"), "en-US");

        // process page - create dangling references
        testHarness.processElement(new CommonWrapper(page), 1000L);

        // dangling state intro
        MapState<String, List<String>> danglingState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getDanglingReferenceStateDescriptor());

        // component state
        MapState<String, PageComponent> pageState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getPageComponentStateDescriptor());

        // Verify dangling references are tracked
        List<String> waitingForComp1 = danglingState.get("comp1");
        assertNotNull(waitingForComp1, "Should have dangling reference for comp1");
        assertEquals(1, waitingForComp1.size());
        assertEquals("page1", waitingForComp1.get(0));

        List<String> waitingForComp2 = danglingState.get("comp2");
        assertNotNull(waitingForComp2, "Should have dangling reference for comp2");
        assertEquals(1, waitingForComp2.size());
        assertEquals("page1", waitingForComp2.get(0));

        List<String> waitingForMissing = danglingState.get("missing");
        assertNotNull(waitingForMissing, "Should have dangling reference for missing component");
        assertEquals(1, waitingForMissing.size());
        assertEquals("page1", waitingForMissing.get(0));

        // page is stored in state but not resolved
        PageComponent storedPage = pageState.get("page1");
        assertNotNull(storedPage, "Page should be stored in state");
        assertEquals("page1", storedPage.pageId);
        assertEquals("landing", storedPage.pageType);
        assertFalse(storedPage.fullyResolved, "Page should not be fully resolved yet");
        assertEquals(3, storedPage.missingComponentIds.size(), "Should have 3 missing components");

        // no output yet
        assertTrue(testHarness.getOutput().isEmpty(), "No output while components missing");

        // first component - resolve one dangling reference
        Component comp1 = createComponent("comp1", "text", List.of());
        testHarness.processElement(new CommonWrapper(comp1), 2000L);

        // dangling reference for comp1 should be removed
        assertNull(danglingState.get("comp1"), "Dangling reference for comp1 should be cleaned up");
        
        // Other dangling references should still exist
        assertNotNull(danglingState.get("comp2"), "Dangling reference for comp2 should still exist");
        assertNotNull(danglingState.get("missing"), "Dangling reference for missing should still exist");

        // Page still not be resolved (missing comp2 and missing)
        storedPage = pageState.get("page1");
        assertFalse(storedPage.fullyResolved, "Page should still not be fully resolved");
        assertEquals(2, storedPage.missingComponentIds.size(), "Should have 2 missing components remaining");

        // no output
        assertTrue(testHarness.getOutput().isEmpty(), "No output while some components still missing");
    }

    @Test
    public void testStateIntrospection_CompleteResolutionAndCleanup() throws Exception {

        Component comp3 = createComponent("comp3", "text", List.of());
        Component comp2 = createComponent("comp2", "container", List.of("comp3"));
        Component comp1 = createComponent("comp1", "container", List.of("comp2"));
        Page page = new Page("page1", "landing", true, List.of("comp1"), "en-US");

        testHarness.processElement(new CommonWrapper(page), 1000L);

        // get state
        MapState<String, Component> componentState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getComponentStateDescriptor());
        MapState<String, List<String>> danglingState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getDanglingReferenceStateDescriptor());
        MapState<String, PageComponent> pageState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getPageComponentStateDescriptor());

        // page stored, dangling reference created, no components
        assertNotNull(pageState.get("page1"), "Page should be in state");
        assertNotNull(danglingState.get("comp1"), "Should have dangling reference for comp1");
        assertNull(componentState.get("comp1"), "comp1 should not be in component state yet");

        // components
        testHarness.processElement(new CommonWrapper(comp3), 2000L);
        testHarness.processElement(new CommonWrapper(comp2), 2001L);

        // state: components stored, dangling reference still exists
        assertNotNull(componentState.get("comp3"), "comp3 should be in component state");
        assertNotNull(componentState.get("comp2"), "comp2 should be in component state");
        assertNotNull(danglingState.get("comp1"), "Should still have dangling reference for comp1");
        assertFalse(pageState.get("page1").fullyResolved, "Page should not be resolved yet");
        assertTrue(testHarness.getOutput().isEmpty(), "No output yet");

        // final component - trigger complete resolution
        testHarness.processElement(new CommonWrapper(comp1), 2002L);

        // fianl state: all components stored, dangling references cleaned up
        assertNotNull(componentState.get("comp1"), "comp1 should be in component state");
        assertNotNull(componentState.get("comp2"), "comp2 should be in component state");  
        assertNotNull(componentState.get("comp3"), "comp3 should be in component state");
        assertNull(danglingState.get("comp1"), "Dangling reference for comp1 should be cleaned up");

        // page fully resolved and produces output
        assertEquals(1, testHarness.getOutput().size(), "Should produce output after complete resolution");

        // output matches state
        PageComponent pageComponent = testHarness.extractOutputValues().get(0);
        assertEquals("page1", pageComponent.pageId);
        assertEquals(1, pageComponent.resolvedComponents.size());
        
        PageComponent.ResolvedComponent root = pageComponent.resolvedComponents.get(0);
        assertEquals("comp1", root.id);
        assertEquals(1, root.childComponents.size());
        assertEquals("comp2", root.childComponents.get(0).id);
        assertEquals(1, root.childComponents.get(0).childComponents.size());
        assertEquals("comp3", root.childComponents.get(0).childComponents.get(0).id);
    }

    @Test
    public void testStateIntrospection_MultiplePages_SharedComponents() throws Exception {

        // Two pages share a component, component arrives after both pages
        Component sharedComp = createComponent("shared", "text", List.of());
        Component uniqueComp = createComponent("unique", "image", List.of());
        Page page1 = new Page("page1", "landing", true, List.of("shared"), "en-US");
        Page page2 = new Page("page2", "about", true, Arrays.asList("shared", "unique"), "en-US");

        testHarness.processElement(new CommonWrapper(page1), 1000L);
        testHarness.processElement(new CommonWrapper(page2), 1001L);

        // get state
        MapState<String, List<String>> danglingState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getDanglingReferenceStateDescriptor());
        MapState<String, PageComponent> pageState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getPageComponentStateDescriptor());

        // initial state: both pages waiting for shared component
        List<String> waitingForShared = danglingState.get("shared");
        assertNotNull(waitingForShared, "Should have dangling reference for shared component");
        assertEquals(2, waitingForShared.size(), "Both pages should be waiting for shared component");
        assertTrue(waitingForShared.contains("page1"), "page1 should be waiting for shared");
        assertTrue(waitingForShared.contains("page2"), "page2 should be waiting for shared");

        List<String> waitingForUnique = danglingState.get("unique");
        assertNotNull(waitingForUnique, "Should have dangling reference for unique component");
        assertEquals(1, waitingForUnique.size(), "Only page2 should be waiting for unique");
        assertEquals("page2", waitingForUnique.get(0));

        // both pages stored but unresolved
        assertNotNull(pageState.get("page1"), "page1 should be in state");
        assertNotNull(pageState.get("page2"), "page2 should be in state");
        assertFalse(pageState.get("page1").fullyResolved, "page1 should not be resolved");
        assertFalse(pageState.get("page2").fullyResolved, "page2 should not be resolved");

        // shared component - should trigger resolution for both pages
        testHarness.processElement(new CommonWrapper(sharedComp), 2000L);

        // page1 resolved, page2 still waiting for unique comp
        assertNull(danglingState.get("shared"), "Dangling reference for shared should be cleaned up");
        assertNotNull(danglingState.get("unique"), "Dangling reference for unique should still exist");
        assertEquals(1, danglingState.get("unique").size(), "Only page2 should still be waiting for unique");

        assertEquals(1, testHarness.getOutput().size(), "Should have output from page1 resolution");

        // unique component - trigger resolution for page2
        testHarness.processElement(new CommonWrapper(uniqueComp), 2001L);

        // dangling references cleaned up
        assertNull(danglingState.get("shared"), "No dangling references for shared");
        assertNull(danglingState.get("unique"), "No dangling references for unique");

        assertEquals(2, testHarness.getOutput().size(), "Should have output from both page resolutions");

        // both outputs contain the shared component
        List<PageComponent> outputs = testHarness.extractOutputValues();
        PageComponent result1 = outputs.get(0);
        PageComponent result2 = outputs.get(1);

        // Find which result is which page
        PageComponent page1Result = result1.pageId.equals("page1") ? result1 : result2;
        PageComponent page2Result = result1.pageId.equals("page2") ? result1 : result2;

        assertEquals(1, page1Result.resolvedComponents.size(), "page1 should have 1 component");
        assertEquals(2, page2Result.resolvedComponents.size(), "page2 should have 2 components");

        // shared component
        String sharedCompInPage1 = page1Result.resolvedComponents.get(0).id;
        boolean foundSharedInPage2 = page2Result.resolvedComponents.stream()
            .anyMatch(comp -> comp.id.equals("shared"));

        assertEquals("shared", sharedCompInPage1, "page1 should contain shared component");
        assertTrue(foundSharedInPage2, "page2 should also contain shared component");
    }

    // we detect circular refs, but still produce output
    @Test 
    public void testStateIntrospection_CircularReferenceDetection() throws Exception {

        // circular reference: comp1 -> comp2 -> comp1
        Component comp1 = createComponent("comp1", "container", List.of("comp2"));
        Component comp2 = createComponent("comp2", "container", List.of("comp1"));
        Page page = new Page("page1", "landing", true, List.of("comp1"), "en-US");

        testHarness.processElement(new CommonWrapper(comp1), 1000L);
        testHarness.processElement(new CommonWrapper(comp2), 2000L);
        testHarness.processElement(new CommonWrapper(page), 3000L);

        // get state
        MapState<String, Component> componentState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getComponentStateDescriptor());
        MapState<String, PageComponent> pageState =
            testHarness.getOperator().getKeyedStateStore().getMapState(processor.getPageComponentStateDescriptor());

        // all components and page stored
        assertNotNull(componentState.get("comp1"), "comp1 should be in component state");
        assertNotNull(componentState.get("comp2"), "comp2 should be in component state");
        
        PageComponent storedPage = pageState.get("page1");
        assertNotNull(storedPage, "Page should be in state");
        
        // detect circular reference
        assertFalse(storedPage.circularReferences.isEmpty(), "Should detect circular references");
        assertTrue(storedPage.circularReferences.contains("comp1") || storedPage.circularReferences.contains("comp2"), 
                  "Should track circular reference component");

        // circular references should NOT prevent output
        assertFalse(testHarness.getOutput().isEmpty(), "Output should still be produced despite circular references");

        // component data should be preserved in state
        Component storedComp1 = componentState.get("comp1");
        assertEquals("comp1", storedComp1.id);
        assertEquals("container", storedComp1.type);
        assertEquals(1, storedComp1.components.size());
        assertEquals("comp2", storedComp1.components.get(0));

        Component storedComp2 = componentState.get("comp2");
        assertEquals("comp2", storedComp2.id);
        assertEquals("container", storedComp2.type);
        assertEquals(1, storedComp2.components.size());
        assertEquals("comp1", storedComp2.components.get(0));
    }

    private Component createComponent(String id, String type, List<String> childComponents) {
        return new Component(id, type, id + "-label", id + "-name", true, childComponents, "en-US");
    }

    private void verifyPageComponentOutput(PageComponent result, String expectedPageId, String expectedType, int expectedComponentCount) throws Exception {
        assertEquals(expectedPageId, result.pageId);
        assertEquals(expectedType, result.pageType);
        assertEquals(expectedComponentCount, result.resolvedComponents.size());
        assertTrue(result.pagePublished);
    }
}