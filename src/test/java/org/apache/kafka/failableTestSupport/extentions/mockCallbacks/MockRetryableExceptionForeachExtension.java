package org.apache.kafka.failableTestSupport.extentions.mockCallbacks;

import org.apache.kafka.failableTestSupport.mocks.mockCallbacks.MockRetryableExceptionForeach;
import org.junit.jupiter.api.extension.*;

public class MockRetryableExceptionForeachExtension implements BeforeEachCallback, ParameterResolver {
    private MockRetryableExceptionForeach<String, String> mockCallback = new MockRetryableExceptionForeach<>();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(MockRetryableExceptionForeach.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return mockCallback;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        mockCallback.getReceivedParameters().clear();
    }
}
