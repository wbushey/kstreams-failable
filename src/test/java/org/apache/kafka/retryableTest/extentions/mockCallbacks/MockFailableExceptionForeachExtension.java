package org.apache.kafka.retryableTest.extentions.mockCallbacks;

import org.apache.kafka.retryableTest.mockCallbacks.MockFailableExceptionForeach;
import org.junit.jupiter.api.extension.*;

public class MockFailableExceptionForeachExtension implements BeforeEachCallback, ParameterResolver {
    private MockFailableExceptionForeach<String, String> mockCallback = new MockFailableExceptionForeach<>();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(MockFailableExceptionForeach.class);
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
