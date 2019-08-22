package org.apache.kafka.retryableTest.extentions.mockCallbacks;

import org.junit.jupiter.api.extension.*;

public class MockRetryableExceptionForeachExtension implements BeforeEachCallback, ParameterResolver {
    private org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach<String, String> mockCallback = new org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach<>();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(org.apache.kafka.retryableTest.mockCallbacks.MockRetryableExceptionForeach.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return mockCallback;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        mockCallback.getReceivedParameters().clear();
    }
}
