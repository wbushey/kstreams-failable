package org.apache.kafka.retryableTest.extentions;

import org.junit.jupiter.api.extension.*;

public class MockForeach implements BeforeEachCallback, ParameterResolver {
    private org.apache.kafka.retryableTest.mockCallbacks.MockForeach<String, String> mockCallback = new org.apache.kafka.retryableTest.mockCallbacks.MockForeach<>();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(org.apache.kafka.retryableTest.mockCallbacks.MockForeach.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return new org.apache.kafka.retryableTest.mockCallbacks.MockForeach<String, String>();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        mockCallback.getReceivedRecords().clear();
    }
}
