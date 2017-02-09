package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.configs.TopicRuleCategory;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.core.lookup.LookupService;
import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.entity.lookup.NSQLookupdAddresses;
import com.youzan.nsq.client.exception.NSQException;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.expect;

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

    public void lookup() throws NSQException {
        lau = partialMockBuilder(LookupAddressUpdate.class)
                .addMockedMethod("getLookup")
                .withConstructor()
                .createMock();
        lau.setUpDefaultSeedLookupConfig(lookups.split(","));
        LookupAddressUpdate.setInstance(lau);

        Topic aTopic = new Topic("JavaTesting-Producer-Base");
        TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Consumer);
        List<String> clusterIds = new ArrayList<>();
        clusterIds.add("sqs-qa");
        List<String> addresses = new ArrayList<>();
        addresses.add("sqs-qa.s.qima-inc.com:4161");
        expect(lau.getLookup(aTopic, category, true, false)).andStubReturn(NSQLookupdAddresses.create(clusterIds, addresses));
        replayAll();

        IPartitionsSelector aPs = lookup.lookup(aTopic, true, category, true, false);
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

        List<String> clusterIds = new ArrayList<>();
        clusterIds.add("sqs-qa.s.qima-inc.com:4161");

        List<String> addresses = new ArrayList<>();
        addresses.add("sqs-qa.s.qima-inc.com:4161");

        List<String> invalidAddresses = new ArrayList<>();
        invalidAddresses.add("sqs-qa.s.qima-inc.com:4161");

        NSQLookupdAddresses badLookupd = NSQLookupdAddresses.create(clusterIds, addresses);
        NSQLookupdAddresses goodLookupd = NSQLookupdAddresses.create(clusterIds, invalidAddresses);

        TopicRuleCategory category = TopicRuleCategory.getInstance(Role.Producer);
        TopicRuleCategory categoryConsume = TopicRuleCategory.getInstance(Role.Consumer);
        expect(lau.getLookup(aTopic, category, true, false)).andReturn(badLookupd).times(2)
                .andReturn(goodLookupd).times(10);

        expect(lau.getLookup(aTopic, categoryConsume, true, false)).andReturn(badLookupd).times(3).
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
