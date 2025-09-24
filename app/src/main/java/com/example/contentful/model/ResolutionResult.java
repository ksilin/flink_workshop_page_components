package com.example.contentful.model;

import java.util.ArrayList;
import java.util.List;

/**
 * track the outcome of page component resolution
 */
public class ResolutionResult {
    public boolean fullyResolved;
    public int resolvedComponentCount;
    public int missingComponentCount;
    public int circularReferenceCount;
    public int maxDepth;
    public List<String> circularReferences;
    public List<String> missingComponents;
    
    public ResolutionResult() {
        this.circularReferences = new ArrayList<>();
        this.missingComponents = new ArrayList<>();
        this.fullyResolved = false;
        this.resolvedComponentCount = 0;
        this.missingComponentCount = 0;
        this.circularReferenceCount = 0;
        this.maxDepth = 0;
    }
    
    public ResolutionResult(boolean fullyResolved, int resolvedCount, int missingCount, 
                           int circularCount, int maxDepth, List<String> circularRefs, 
                           List<String> missingComps) {
        this.fullyResolved = fullyResolved;
        this.resolvedComponentCount = resolvedCount;
        this.missingComponentCount = missingCount;
        this.circularReferenceCount = circularCount;
        this.maxDepth = maxDepth;
        this.circularReferences = new ArrayList<>(circularRefs);
        this.missingComponents = new ArrayList<>(missingComps);
    }

    public String getSummary() {
        return String.format("Resolution: resolved=%d, missing=%d, circular=%d, depth=%d, shouldOutput=%b",
                resolvedComponentCount, missingComponentCount, circularReferenceCount, 
                maxDepth, isFullyResolved());
    }
    
    public boolean isFullyResolved() {
        return fullyResolved;
    }
    
    public int getResolvedCount() {
        return resolvedComponentCount;
    }
    
    public int getMissingCount() {
        return missingComponentCount;
    }
    
    public int getCircularCount() {
        return circularReferenceCount;
    }
    
    public int getResolutionDepth() {
        return maxDepth;
    }
    
    public List<String> getMissingComponentIds() {
        return missingComponents;
    }
    
    public List<String> getCircularReferences() {
        return circularReferences;
    }
}