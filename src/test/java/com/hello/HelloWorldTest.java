package com.hello;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HelloWorldTest {
    private HelloWorld subject;

    @BeforeEach
    void beforeEach() {
        subject = new HelloWorld();
    }

    @Test
    @DisplayName("Hello Test")
    void helloWorld(){
        assertEquals("Hello World", subject.greeting());
    }
}
