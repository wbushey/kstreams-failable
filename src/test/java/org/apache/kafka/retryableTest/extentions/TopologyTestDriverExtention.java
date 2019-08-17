package org.apache.kafka.retryableTest.extentions;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class TopologyTestDriverExtention implements AfterEachCallback, ParameterResolver{
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
    public void afterEach(ExtensionContext context) throws Exception {
        resetTopologyProps();
        closeDriverIfDefined(context);
    }

    private void resetTopologyProps(){
        topologyProps.clear();
        topologyProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        topologyProps.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        topologyProps.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    }

    private void closeDriverIfDefined(ExtensionContext context) throws IllegalAccessException {
        try {
            // Get instance of the Test Class that is being extended
            Object testInstance = context.getTestInstance().get();

            // Get the field with the TopologyTestDriver, assumed to be a field named 'driver'
            Field driverField = testInstance.getClass().getDeclaredField("driver");

            closeDriverAtFieldOnInstance(driverField, testInstance);
        } catch (NoSuchFieldException e){
            // NoOp - Don't do anything if 'driver' isn't define do the test
        }
    }

    private void closeDriverAtFieldOnInstance(Field driverField, Object testInstance){
        TopologyTestDriver driver  = getTopologyTestDriver(driverField, testInstance);

        if (driver == null){
            return;
        }

        try {
            driver.close();
        } catch (IllegalStateException e){
            // NoOp - The driver was already closed
        }

    }

    private TopologyTestDriver getTopologyTestDriver(Field driverField, Object testInstance){
        if (driverField.getType() != TopologyTestDriver.class){
            return null;
        }

        driverField.setAccessible(true);

        TopologyTestDriver driver = null;
        try {
            driver = (TopologyTestDriver) driverField.get(testInstance);
        } catch (IllegalAccessException e) {
            // Unexpected due to setAccessible(true) above
        }

        return driver;
    }
}
