package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.configs.DCCSeedLookupdConfig;
import com.youzan.nsq.client.configs.TopicRuleCategory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.entity.lookup.AbstractSeedLookupdConfig;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddress;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.util.IOUtil;
import org.easymock.EasyMockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.same;

@Test(priority = 3)
public class ITLookup extends EasyMockSupport {
    private static final Logger logger = LoggerFactory.getLogger(ITLookup.class);
    private String lookups;
    private LookupService lookup;
    private LookupAddressUpdate lau;

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        lookups = props.getProperty("lookup-addresses");
        lookup = new LookupServiceImpl(Role.Producer);

    }

    public void lookup() throws NSQLookupException {
        lau = partialMockBuilder(LookupAddressUpdate.class)
                .addMockedMethod("getLookup")
                .withConstructor()
                .createMock();
        lau.setUpDefaultSeedLookupConfig(lookups.split(","));
        LookupAddressUpdate.setInstance(lau);

        Topic aTopic = new Topic("JavaTesting-Producer-Base");
        TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Consumer);
        expect(lau.getLookup(aTopic, category, true)).andStubReturn(NSQLookupdAddress.create("sqs-qa", "sqs-qa.s.qima-inc.com:4161"));
        replayAll();

        IPartitionsSelector aPs = lookup.lookup(aTopic, true, category, true);
        Assert.assertNotNull(aPs);
        verifyAll();
    }

    //test with invalid lookup address in listlookup API
    @Test
    public void testInvalidLookupAddress() throws NSQException, InterruptedException {
        Topic aTopic = new Topic("JavaTesting-Producer-Base");

        lau = partialMockBuilder(LookupAddressUpdate.class)
                .addMockedMethod("getLookup")
                .withConstructor()
                .createMock();
        lau.setUpDefaultSeedLookupConfig(lookups.split(","));
        LookupAddressUpdate.setInstance(lau);

        NSQLookupdAddress badLookupd = NSQLookupdAddress.create("sqs-qa.s.qima-inc.com:4161", "sqs-qa.which.is.invalid:4161");
        NSQLookupdAddress goodLookupd = NSQLookupdAddress.create("sqs-qa.s.qima-inc.com:4161", "sqs-qa.s.qima-inc.com:4161");

        TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Producer);
        TopicRuleCategory categoryConsume = TopicRuleCategory.getInstance(Role.Consumer);
        expect(lau.getLookup(aTopic, category, true)).andReturn(badLookupd).times(2)
                .andReturn(goodLookupd).times(10);

        expect(lau.getLookup(aTopic, categoryConsume, true)).andReturn(badLookupd).times(3).
                andReturn(goodLookupd).anyTimes();
        replayAll();


        NSQConfig configPro = new NSQConfig("sqs-qa.s.qima-inc.com:4161");
        configPro.setUserSpecifiedLookupAddress(true);
        Producer producer = new ProducerImplV2(configPro);
        producer.start();
        for(int i = 0; i < 10; i++){
            Message msg = Message.create(aTopic, "message " + i);
            producer.publish(msg);
            logger.info("message sent");
        }
        producer.close();
        final CountDownLatch latch = new CountDownLatch(10);
        NSQConfig config = new NSQConfig("BaseConsumer");
        config.setLookupAddresses("sqs-qa.s.qima-inc.com:4161");
        config.setUserSpecifiedLookupAddress(true);
        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                latch.countDown();
            }
        });
        consumer.subscribe(aTopic);
        consumer.start();
        Assert.assertTrue(latch.await(240, TimeUnit.SECONDS));
        consumer.close();
        resetAll();
    }

    @AfterMethod
    public void close() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        IOUtil.closeQuietly(lookup);
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(this.lau);
        LookupAddressUpdate.setInstance(null);
        this.lau = null;
    }

}
