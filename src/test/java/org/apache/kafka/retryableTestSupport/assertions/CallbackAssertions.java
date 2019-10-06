package org.apache.kafka.retryableTestSupport.assertions;

import org.apache.kafka.retryableTestSupport.Pair;
import org.apache.kafka.retryableTestSupport.mocks.mockCallbacks.MockCallback;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CallbackAssertions {

    /**
     * Decorates a MockCallback with methods that assert expectations.
     * @param callback
     * @return
     */
    public static CallbackAssertions expect(MockCallback<String, String> callback){
        return new CallbackAssertions(callback);
    }

    public void toHaveReceivedExactlyOneCall(Pair<String, String> expectedCall){
        toHaveReceivedExactlyCalls(Collections.singletonList(expectedCall));
    }

    public void toHaveReceivedExactlyCalls(List<Pair<String, String>> parameters){
        assertEquals(parameters.size(), callback.getReceivedParameters().size());
        parameters.forEach(parameter -> {
            assertTrue(callback.getReceivedParameters().contains(parameter));
        });
    }

    private final MockCallback<String, String> callback;

    private CallbackAssertions(MockCallback<String, String> callback){
        this.callback = callback;
    }
}
