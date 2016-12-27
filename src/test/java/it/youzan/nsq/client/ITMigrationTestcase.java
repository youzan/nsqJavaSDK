package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.*;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 16/11/15.
 */
public class ITMigrationTestcase {
    private static final Logger logger = LoggerFactory.getLogger(ITMigrationTestcase.class);
    private static Properties props;

    //old config points to nsq cluster, for producer only
    private NSQConfig oldConfig;
    //new config points to sqs cluster, for producer and consumer
    private NSQConfig newConfig;
    private ConfigAccessAgent agent = null;

    @BeforeMethod
    public void init() throws IOException {
        props = new Properties();
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        //initialize old config
        String oldLookupAddresses = props.getProperty("old-lookup-addresses");
        String newLookupAddresses = props.getProperty("new-lookup-addresses");

        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        oldConfig = new NSQConfig(oldLookupAddresses);
        oldConfig.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        oldConfig.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        oldConfig.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        oldConfig.setUserSpecifiedLookupAddress(true);

        newConfig = new NSQConfig("BaseConsumer");
        newConfig.setLookupAddresses(newLookupAddresses);
        newConfig.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        newConfig.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        newConfig.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        newConfig.setUserSpecifiedLookupAddress(true);
    }

    @Test
    public void test() throws NSQException, InterruptedException {
        agent = ConfigAccessAgent.getInstance();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger oldCnt = new AtomicInteger(0);
        final AtomicInteger newCnt = new AtomicInteger(0);
        //1. consumer subscribe to sqs lookup address
        Consumer consumer = new ConsumerImplV2(newConfig, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Got: " + message.getReadableContent());
                if(message.getReadableContent().contains("Old")){
                    oldCnt.incrementAndGet();
                }else if(message.getReadableContent().contains("New")){
                    newCnt.incrementAndGet();
                }
                if(oldCnt.get() + newCnt.get() == 200) {
                    logger.info("Received: Old cluster: " + oldCnt.get() + ", New cluster: " + newCnt.get());
                    latch.countDown();
                }
            }
        });
        consumer.subscribe("JavaTesting-Migration");
        consumer.start();
        CountDownLatch waitLatch = new CountDownLatch(1);
        waitLatch.await(2, TimeUnit.MINUTES);

        //2. producer send message to nsq cluster;
        Producer producer2Old  = new ProducerImplV2(oldConfig);
        producer2Old.start();
        for(int i=0; i< 100; i++) {
            String msgTxt = "Old message: #" + i;
            producer2Old.publish(msgTxt.getBytes(Charset.defaultCharset()), "JavaTesting-Migration");
        }
        producer2Old.close();

        //3. producer send message to sqs cluster;
        Producer producer2New  = new ProducerImplV2(newConfig);
        producer2New.start();
        for(int i=0; i< 100; i++) {
            String msgTxt = "New message: #" + i;
            producer2New.publish(msgTxt.getBytes(Charset.defaultCharset()), "JavaTesting-Migration");
        }
        producer2New.close();
        //await
        Assert.assertTrue(latch.await(2, TimeUnit.MINUTES));
        consumer.close();
    }

    private final static String configStrConsumer = "{" +
            "\"previous\":[\"http://nsq-dev.s.qima-inc.com:4161\"]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "}" +
            "}";

    //config 1 stands for normal status
    private final static String configStr1 = "{" +
            "\"previous\":[]," +
            "\"current\":[\"http://nsq-dev.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "}" +
            "}";

    private final static String configStr2 = "{" +
            "\"previous\":[\"http://nsq-dev.s.qima-inc.com:4161\"]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":10.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    private final static String configStr3 = "{" +
            "\"previous\":[\"http://nsq-dev.s.qima-inc.com:4161\"]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":50.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    private final static String configStr4 = "{" +
            "\"previous\":[\"http://nsq-dev.s.qima-inc.com:4161\"]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":100.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    private final static String configStr5 = "{" +
            "\"previous\":[]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "}" +
            "}";

    private String[] migrationConfigs = new String[]{configStr1, configStr2, configStr3, configStr4, configStr5};

    @Test
    public void testMigrationAccordingToSeedConfig() throws NSQException, InterruptedException {
        agent = ConfigAccessAgent.getInstance();

        NSQConfig configConsumer = new NSQConfig("BaseConsumer");
        configConsumer.setRdy(1);
        NSQConfig configProduer = new NSQConfig();

        agent = ConfigAccessAgent.getInstance();
        final SortedMap<String, String> valueMapPro = new TreeMap<>();
        final SortedMap<String, String> valueMapCon = new TreeMap<>();
        valueMapPro.put("JavaTesting-Migration", migrationConfigs[0]);
        valueMapCon.put("JavaTesting-Migration", configStrConsumer);

        final Topic topic = new Topic("JavaTesting-Migration");

        DCCMigrationConfigAccessDomain domain = (DCCMigrationConfigAccessDomain) DCCMigrationConfigAccessDomain.getInstance(topic);
        Role aRole = Role.getInstance("producer");
        DCCMigrationConfigAccessKey keyProducer = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRole);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMapPro, true);

        Role aRoleConsumer = Role.getInstance("consumer");
        DCCMigrationConfigAccessKey keyConsumer = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRoleConsumer);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyConsumer}, valueMapCon, true);

        final CountDownLatch phase1 = new CountDownLatch(1);
        final CountDownLatch phase2 = new CountDownLatch(1);
        final CountDownLatch phase3 = new CountDownLatch(1);
        final CountDownLatch phase4 = new CountDownLatch(1);
        final CountDownLatch phase5 = new CountDownLatch(1);

        //create producer and consumer
        final AtomicLong cnt = new AtomicLong();
        final AtomicLong cntPro = new AtomicLong();
        Consumer consumer = new ConsumerImplV2(configConsumer, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                long count = cnt.incrementAndGet();
                if(count >= 10){
                    phase1.countDown();
                }
                if(count >= 20){
                    phase2.countDown();
                }
                if(count >= 30){
                    phase3.countDown();
                }
                if(count >= 40){
                    phase4.countDown();
                }
                if(count >= 50){
                    phase5.countDown();
                }
            }
        });

        consumer.subscribe(topic);
        consumer.start();

        final Producer producer = new ProducerImplV2(configProduer);
        producer.start();
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.submit(new Runnable() {
            @Override
            public void run() {
                Message msg = Message.create(topic, "message migration");
                try {
                    while(true) {
                        producer.publish(msg);
                        cntPro.incrementAndGet();
                        Thread.sleep(2000);
                    }
                } catch (NSQException | InterruptedException e) {
                    Assert.fail("Error in publish", e);
                }
            }
        });

        Assert.assertTrue(phase1.await(60, TimeUnit.SECONDS));
        //phase 2, update config
        valueMapPro.clear();
        valueMapPro.put("JavaTesting-Migration", migrationConfigs[1]);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMapPro, true);

//        Assert.assertTrue(phase2.await(60, TimeUnit.SECONDS));
        phase2.await();
        //phase 3, update config
        valueMapPro.clear();
        valueMapPro.put("JavaTesting-Migration", migrationConfigs[2]);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMapPro, true);

//        Assert.assertTrue(phase3.await(60, TimeUnit.SECONDS));
        phase3.await();
        //phase 4, update config
        valueMapPro.clear();
        valueMapPro.put("JavaTesting-Migration", migrationConfigs[3]);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMapPro, true);

        Assert.assertTrue(phase4.await(60, TimeUnit.SECONDS));

        //phase 5, update config final state
        valueMapPro.clear();
        valueMapPro.put("JavaTesting-Migration", migrationConfigs[4]);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMapPro, true);

        Assert.assertTrue(phase4.await(60, TimeUnit.SECONDS));

        exec.shutdownNow();
        producer.close();
        Thread.sleep(10000);
        consumer.close();
        Assert.assertEquals(cnt.get(), cntPro.get());
    }

    @AfterMethod
    public void release() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        System.clearProperty("nsq.sdk.configFilePath");
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(agent);
    }
}
