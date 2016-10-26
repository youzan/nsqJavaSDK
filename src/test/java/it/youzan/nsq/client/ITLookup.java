package it.youzan.nsq.client;

import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Properties;

@Test(priority = 3)
public class ITLookup {
    private static final Logger logger = LoggerFactory.getLogger(ITLookup.class);

    private LookupService lookup;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String lookups = props.getProperty("lookup-addresses");
        lookup = new LookupServiceImpl(lookups, Role.Producer);
    }

    public void lookup() throws NSQLookupException {
        lookup.lookup("JavaTesting-Producer-Base", true);
    }

    @AfterClass
    public void close() {
        IOUtil.closeQuietly(lookup);
    }

}
