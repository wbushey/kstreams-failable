package org.apache.kafka.retryableTest.extentions.topologyTestDriver;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.*;

import java.util.Properties;

public class TopologyTestDriverExtension implements AfterEachCallback, ParameterResolver, TestInstancePostProcessor{
    private final Properties topologyProps = new Properties();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().equals(Properties.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        resetTopologyProps();
        return topologyProps;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        resetTopologyProps();
        closeDriver(context);
    }

    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) throws Exception {
        if (!isValidTestClass(testInstance)){
            throw new Exception("Tests ExtendedWith TopologyTestDriverExtension must implement TopologyTestDriverProvider");
        }
    }


    private Object getTestInstance(ExtensionContext context){
        return context.getTestInstance().get();
    }

    private boolean isValidTestClass(Object testInstance){
        return TopologyTestDriverProvider.class.isAssignableFrom(testInstance.getClass());
    }

    private void resetTopologyProps(){
        topologyProps.clear();
        topologyProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        topologyProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    }

    private void closeDriver(ExtensionContext context){
        Object testInstance = getTestInstance(context);
        TopologyTestDriver driver  = getTopologyTestDriver(testInstance);

        if (driver == null){
            return;
        }

        try {
            driver.close();
        } catch (IllegalStateException e){
            // NoOp - The driver was already closed
        }

    }

    private TopologyTestDriver getTopologyTestDriver(Object testInstance){
        TopologyTestDriver driver = null;
        if (testInstance instanceof TopologyTestDriverProvider){
            driver = ((TopologyTestDriverProvider)testInstance).getTopologyTestDriver();
        }
        return driver;
    }

}
