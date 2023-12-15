package com.onehouse;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class GlueWrapperMainTest {

    @Test
    @SneakyThrows
    public void testMain() {
        try {
            GlueWrapperMain.main(new String[]{});
        } catch (Exception e) {
            fail(e);
        }
    }

}