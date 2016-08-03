package it.youzan.nsq.client;

import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

@Test
public class ITLookup {

    private static final Logger logger = LoggerFactory.getLogger(ITLookup.class);

    private final Random random = new Random();

    @BeforeClass
    public void init() throws Exception {
        logger.info("Now initialize {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        final String lookups = props.getProperty("lookup-addresses");


        logger.debug("The environment is {} .", env);
    }

}
