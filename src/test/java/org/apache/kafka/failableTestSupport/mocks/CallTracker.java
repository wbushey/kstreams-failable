package org.apache.kafka.failableTestSupport.mocks;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CallTracker {
    private final Map<String, Integer> callCounts = new HashMap<>();
    private final List<String> methodNames;

    public CallTracker(List<String> methodNames){
        this.methodNames = methodNames;
        resetCallCounts();
    }

    /**
     * Resets all method call counts to 0.
     */
    public void resetCallCounts(){
        methodNames.forEach(methodName -> {
            callCounts.put(methodName, 0);
        });

    }

    public void incrementCallCount(String methodName){
        Integer methodCallCount = callCounts.get(methodName);
        callCounts.put(methodName, methodCallCount + 1);
    }

    /**
     * Returns the number of times the provided method name has been called across
     * all instances of MockDefaultSerde.
     * @param methodName
     * @return
     */
    public Integer getCallCount(String methodName){
        return callCounts.get(methodName);
    }
}
