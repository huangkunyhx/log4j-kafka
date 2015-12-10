package com.unionpaysmart.log4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hello world!
 *
 */
public class Test{
    private static Logger LOGGER = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws InterruptedException {
        //LOGGER.debug("aaaaa");
        for (int i = 0; i < 10000; i++) {
            LOGGER.debug("aaaaa" + i);
            LOGGER.error("aaaaa" + i);
            LOGGER.info("aaaaa" + i);
//            try {
//                f();
//            } catch (Exception e) {
//                LOGGER.error("ssss", e);
//            }
        }

    }
    
    public static void f() {
        int L = 1/0;
        L = L + 1;
    }

}
