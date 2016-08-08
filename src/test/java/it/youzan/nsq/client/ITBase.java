package it.youzan.nsq.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

@Test
public class ITBase {
    private static final Logger logger = LoggerFactory.getLogger(ITBase.class);

    @BeforeClass
    public void init() throws Exception {
        logger.info("Now initialize {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        logger.debug("The environment is {} .", env);
    }

    public void test() {
        
    }
}
