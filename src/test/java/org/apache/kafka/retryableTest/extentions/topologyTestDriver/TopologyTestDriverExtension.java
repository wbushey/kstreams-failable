package org.apache.kafka.retryableTest.extentions.topologyTestDriver;

import org.apache.kafka.retryableTest.extentions.TopologyPropertiesExtension;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;


public class TopologyTestDriverExtension
        extends TopologyPropertiesExtension
        implements AfterEachCallback, BeforeEachCallback, TestInstancePostProcessor {

    @Override
    public void beforeEach(ExtensionContext context){
        createTopologyTestDriver(context);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        closeDriver(context);
    }

    @Override
    public void postProcessTestInstance(Object testInstance, ExtensionContext context) throws Exception {
        if (!isValidTestClass(testInstance)){
            throw new Exception("Tests ExtendedWith TopologyTestDriverExtension must implement WithTopologyTestDriver");
        }
    }

    private Object getTestInstance(ExtensionContext context){
        return context.getTestInstance().get();
    }

    private boolean isValidTestClass(Object testInstance){
        return WithTopologyTestDriver.class.isAssignableFrom(testInstance.getClass());
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
        if (testInstance instanceof WithTopologyTestDriver){
            driver = ((WithTopologyTestDriver)testInstance).getTopologyTestDriver();
        }
        return driver;
    }

    private void createTopologyTestDriver(ExtensionContext context){
        Object testInstance = getTestInstance(context);
        if (testInstance instanceof WithTopologyTestDriver){
            ((WithTopologyTestDriver)testInstance).createTopologyTestDriver();
        }
    }

}
