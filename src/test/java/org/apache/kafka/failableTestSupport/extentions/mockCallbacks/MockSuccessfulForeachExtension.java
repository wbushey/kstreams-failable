package org.apache.kafka.failableTestSupport.extentions.mockCallbacks;

import org.apache.kafka.failableTestSupport.mocks.mockCallbacks.MockSuccessfulForeach;
import org.junit.jupiter.api.extension.*;

public class MockSuccessfulForeachExtension implements BeforeEachCallback, ParameterResolver {
    private MockSuccessfulForeach<String, String> mockCallback = new MockSuccessfulForeach<>();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(MockSuccessfulForeach.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return new MockSuccessfulForeach<String, String>();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        mockCallback.getReceivedParameters().clear();
    }
}
