package it.youzan.nsq.client;

import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

@Test(groups = "ITProducer")
public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    private final Random random = new Random();
    private NSQConfig config;

    @BeforeClass
    public void init() throws NSQException, IOException {
        logger.info("Now init {} at {} .", this.getClass().getName(), System.currentTimeMillis());
    }

    @DataProvider(name = "topics", parallel = true)
    public Object[][] createData() {
        return new Object[][] { { "" }, { "" }, { "" } };
    }
}
