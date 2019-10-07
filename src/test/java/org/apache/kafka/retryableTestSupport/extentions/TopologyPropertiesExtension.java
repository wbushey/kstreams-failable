package org.apache.kafka.retryableTestSupport.extentions;

import org.junit.jupiter.api.extension.*;

import java.util.Properties;

import static org.apache.kafka.retryableTestSupport.TopologyFactory.createTopologyProps;
import static org.apache.kafka.retryableTestSupport.TopologyFactory.resetTopologyProps;

public class TopologyPropertiesExtension implements AfterEachCallback, ParameterResolver {
    private final Properties topologyProps = createTopologyProps();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Properties.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        resetTopologyProps(topologyProps);
        return topologyProps;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        resetTopologyProps(topologyProps);
    }
}
