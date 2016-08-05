package it.youzan.nsq.client;

import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.exception.NSQLookupException;
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
    private LookupService lookup;

    @BeforeClass
    public void init() throws Exception {
        logger.info("Now initialize {} at {} .", this.getClass().getName(), System.currentTimeMillis());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String lookups = props.getProperty("lookup-addresses");
        lookup = new LookupServiceImpl(lookups);
    }

    public void lookup() throws NSQLookupException {
        lookup.lookup("JavaTesting-Producer-Base", true);
    }

}
