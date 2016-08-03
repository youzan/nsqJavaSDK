package it.youzan.nsq.client;

import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by linzuxiong on 08/03/2016.
 */
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
}
