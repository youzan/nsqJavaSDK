package com.youzan.nsq.client.dcc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.ConfigRequest;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.exceptions.InvalidConfigException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.TraceConfigAgent;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/9/21.
 */
public class TraceConfigAgentTest extends AbstractNSQClientTestcase{

    private static Logger logger = LoggerFactory.getLogger(TraceConfigAgentTest.class);
    private Properties props;
    private final String topic = "JavaTesting-Producer-Base";
    private final String channel = "BaseConsumer";
    private String nsqdUrl;
    private ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public void init() throws IOException {
        super.init();
        props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("configClient.properties")) {
            props.load(is);
        }

        JsonNode lookup = mapper.readTree(new URL("http://" + props.getProperty("lookup-addresses") + "/lookup?topic=" + this.topic));
        JsonNode nodes = lookup.get("data")
                .get("producers");
        assert nodes.isArray();
        for(JsonNode node : nodes){
            nsqdUrl = "http://" + node.get("broadcast_address").asText() + ":" + node.get("http_port").asText();
            //break, there should be only one
            break;
        }
    }

    @Test
    /**
     * verify that config agent manager is fetched
     */
    public void testGetConfigAgent(){
        NSQConfig.setUrls(props.getProperty("urls"));
        NSQConfig.setConfigAgentEnv(props.getProperty("configAgentEnv"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));
        //initialize trace config agent
        TraceConfigAgent.getInstance();
        Assert.assertEquals(NSQConfig.getConfigAgentEnv(), props.getProperty("configAgentEnv"));
        Assert.assertEquals(NSQConfig.getUrls(), props.getProperty("urls").split(","));
        logger.info(NSQConfig.getTraceAgentConfig().toString());
    }

    @Test
    /**
     * test
     */
    public void testIsTraceOn() throws IOException, IllegalAccessException, ConfigParserException, InterruptedException {
        CountDownLatch latch = publishTraceConfig("[{\"key\":\"TestTrace1\",\"value\":\"true\"},{\"key\":\"TestTrace2\",\"value\":\"false\"}]");
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        NSQConfig.setUrls(props.getProperty("urls"));
        NSQConfig.setConfigAgentEnv(props.getProperty("configAgentEnv"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));
        TraceConfigAgent cAgentMgr = TraceConfigAgent.getInstance();
        logger.info("Trace agent sleeps for 3 sec to wait for subscribe onChanged update...");
        Thread.sleep(3000L);
        logger.info("Trace agent awakes.");
        Assert.assertTrue(
                cAgentMgr.checkTraced(new Topic("TestTrace1", 1))
        );
        Assert.assertTrue(
                cAgentMgr.checkTraced(new Topic("TestTrace1", 2))
        );
        Assert.assertFalse(
                cAgentMgr.checkTraced(new Topic("TestTrace2", 1))
        );
        Assert.assertFalse(
                cAgentMgr.checkTraced(new Topic("TestTrace2", 2))
        );

        latch = publishTraceConfig("[{\"key\":\"TestTrace1\",\"value\":\"false\"},{\"key\":\"TestTrace2\",\"value\":\"true\"}]");
        Assert.assertTrue(latch.await(3, TimeUnit.SECONDS));
        logger.info("Trace agent sleeps for 3 sec to wait for subscribe onChanged update...");
        Thread.sleep(3000L);
        logger.info("Trace agent awakes.");
        Assert.assertTrue(
                cAgentMgr.checkTraced(new Topic("TestTrace2", 1))
        );
        Assert.assertTrue(
                cAgentMgr.checkTraced(new Topic("TestTrace2", 2))
        );
        Assert.assertFalse(
                cAgentMgr.checkTraced(new Topic("TestTrace1", 1))
        );
        Assert.assertFalse(
                cAgentMgr.checkTraced(new Topic("TestTrace1", 2))
        );
        cAgentMgr.close();
    }

    private boolean emptyChannelTopic(String channel, String topic) throws IOException {
        URL url = new URL(this.nsqdUrl + "/channel/empty?channel=" + channel + "&topic=" + topic);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();

        con.setRequestMethod("POST");
        int respCode =  con.getResponseCode();
        return respCode == 200;
    }

    private CountDownLatch deleteLookupAddress(final String app, final String key, final String env, final String[] subkeys) throws IOException, IllegalAccessException, ConfigParserException, InvalidConfigException {
        //create publisher to dcc remote to upload combination config
        ConfigClient client = ConfigClientBuilder.create()
                .setRemoteUrls(props.getProperty("urls").split(","))
                .setClientConfig(new ClientConfig())
                .setBackupFilePath(props.getProperty("backupFilePath"))
                .setConfigEnvironment(props.getProperty("configAgentEnv"))
                .build();

        //build lookupaddress
        List<ConfigRequest> requests = new ArrayList<>();
        for(int i=0; i < subkeys.length; i++){
            ConfigRequest request = (ConfigRequest) ConfigRequest.create(client)
                    .setApp(app)
                    .setKey(key)
                    .setSubkey(subkeys[i])
                    .build();
            requests.add(request);
        }
        final CountDownLatch latch = new CountDownLatch(1);
        client.delete(new IResponseCallback() {
            @Override
            public void onChanged(List<Config> list) throws Exception {
                logger.info("Publish successfully.");
                latch.countDown();
            }

            @Override
            public void onFailed(List<Config> list, Exception e) throws Exception {
                logger.error("Publish to dcc failed. ", e);
                Assert.fail("Publish to dcc failed");
            }
        }, requests);

        return latch;
    }

    private CountDownLatch publishLookupAddress(final String app, final String key, final String env, final String[] lookupAddresses) throws IOException, IllegalAccessException, ConfigParserException {
        //create publisher to dcc remote to upload combination config
        ConfigClient client = ConfigClientBuilder.create()
                .setRemoteUrls(props.getProperty("urls").split(","))
                .setClientConfig(new ClientConfig())
                .setBackupFilePath(props.getProperty("backupFilePath"))
                .setConfigEnvironment(props.getProperty("configAgentEnv"))
                .build();

        //build lookupaddress
        StringBuilder sb = new StringBuilder();
        ObjectMapper mapper = SystemUtil.getObjectMapper();
        ArrayNode array = mapper.createArrayNode();
        for(int i=0; i<lookupAddresses.length; i++){
            ObjectNode node = mapper.createObjectNode();
            node.put("key", "addr"+i);
            node.put("value", lookupAddresses[i]);
            array.add(node);
        }
        //config for trace
        Config traceConfig = (Config) Config.create()
                .setApp(app)
                .setKey(key)
                .setEnv(env)
                .setValue(Config.ConfigType.COMBINATION, array.toString())
                .build();
        List<Config> configs = new ArrayList<>();
        configs.add(traceConfig);
        final CountDownLatch latch = new CountDownLatch(1);
        client.publish(new IResponseCallback() {
            @Override
            public void onChanged(List<Config> list) throws Exception {
                logger.info("Publish successfully.");
                latch.countDown();
            }

            @Override
            public void onFailed(List<Config> list, Exception e) throws Exception {
                logger.error("Publish to dcc failed. ", e);
                Assert.fail("Publish to dcc failed");
            }
        }, configs);

        return latch;
    }

    private CountDownLatch publishTraceConfig(String traceContent) throws IOException, IllegalAccessException, ConfigParserException {
        //create publisher to dcc remote to upload combination config
        ConfigClient client = ConfigClientBuilder.create()
                .setRemoteUrls(props.getProperty("urls").split(","))
                .setClientConfig(new ClientConfig())
                .setBackupFilePath(props.getProperty("backupFilePath"))
                .setConfigEnvironment(props.getProperty("configAgentEnv"))
                .build();
        //config for trace
        Config traceConfig = (Config) Config.create()
                .setApp("nsq")
                .setKey("trace")
                .setEnv(props.getProperty("configAgentEnv"))
                .setValue(Config.ConfigType.COMBINATION, traceContent)
                .build();
        List<Config> configs = new ArrayList<>();
        configs.add(traceConfig);
        final CountDownLatch latch = new CountDownLatch(1);
        client.publish(new IResponseCallback() {
            @Override
            public void onChanged(List<Config> list) throws Exception {
                logger.info("Publish successfully.");
                latch.countDown();
            }

            @Override
            public void onFailed(List<Config> list, Exception e) throws Exception {
                logger.error("Publish to dcc failed. ", e);
                Assert.fail("Publish to dcc failed");
            }
        }, configs);

        return latch;
    }

    @Test
    //create producer with trace on, publish some message
    public void testPublishTrace() throws IOException, NSQException, ConfigParserException, IllegalAccessException, InterruptedException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().toString()+"#testPublishTrace");
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        //config trace config agent properties
        testGetConfigAgent();

        //publish trace config
        CountDownLatch latch = publishTraceConfig("[{\"key\":\"JavaTesting-Producer-Base\",\"value\":\"true\"},{\"key\":\"TestTrace2\",\"value\":\"true\"}]");
        Assert.assertTrue(emptyChannelTopic(this.channel, this.topic));
        latch.await(3, TimeUnit.SECONDS);

        NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));

        Producer producer = new ProducerImplV2(config);
        producer.start();
        //set ID
        producer.setTraceID(2L);
        int messageNum = 10;
        for(int i=0; i< messageNum; i++) {
            producer.publish(("message #" + i).getBytes(), "JavaTesting-Producer-Base");
        }
        producer.close();

        int actual = consumeMessages(new Topic("JavaTesting-Producer-Base"), messageNum);
        Assert.assertEquals(actual, messageNum);
    }

    /**
     * help function to consume messages for specified topic
     * @return actual number of messages consumer received.
     */
    private int consumeMessages(Topic topic, int expectedNum) throws IOException, NSQException, InterruptedException {
        //initialize properties
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().toString()+"#testPublishTrace");
        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        NSQConfig config = new NSQConfig();
        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
        config.setConsumerName("BaseConsumer");

        final CountDownLatch latch = new CountDownLatch(expectedNum);

        final AtomicInteger received = new AtomicInteger(0);
        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                received.incrementAndGet();
                logger.info("receive message : {}", message.toString());
                logger.info("message body: {}. ", message.getReadableContent());
                latch.countDown();
            }
        });

        consumer.setAutoFinish(true);
        consumer.subscribe(topic.getPartitionId(), topic.getTopicText());
        consumer.start();
        latch.await(expectedNum * 6, TimeUnit.SECONDS);
        logger.info("Consumer received {} messages in total.", received.get());
        return received.get();
    }

    @Test
    /**
     * publish trace with bad urls, then nsq sdk will switch to backup lookup address
     */
    public void testTraceWithBadURL() throws NSQException {
        String badURL = "http://thisisbadurl:4161";
        String backupPath = "/tmp/nsqtest/makebadofdccurl.bak";
        NSQConfig.setUrls(badURL);
        NSQConfig.setConfigAgentBackupPath(backupPath);
        NSQConfig config = getNSQConfig();
        config.setLookupAddresses("http://sqs-qa.s.qima-inc.com:4161");

        Producer producer = TraceConfigAgentTest.createProducer(config);
        producer.setTraceID(2049L);
        producer.start();
        String msg = "testbaddccurl";
        for(int i=0; i<10; i++)
            producer.publish(msg.getBytes(), "JavaTesting-Trace");
        producer.close();
    }

    @Test
    /**
     * update lookup address update
     */
    public void testLookupAddressUpdateWBadUrls(){
        //update dcc properties
        NSQConfig.setUrls("http://thisisbadlookup:4161");
        NSQConfig.setConfigAgentEnv(props.getProperty("configAgentEnv"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));

        config.setLookupAddresses("http://sqs-qa.s.qima-inc.com:4161");

        LookupAddressUpdate lookupUpdate = new LookupAddressUpdate(config);
        String[] lookupAddr = lookupUpdate.getNewLookupAddress();
        Assert.assertEquals(lookupAddr[0], "http://sqs-qa.s.qima-inc.com:4161");
    }

    @Test
    /**
     * update lookup address update
     */
    public void testLookupAddressUpdate() throws NoSuchFieldException, IllegalAccessException {
        //update dcc properties
        NSQConfig.setUrls(props.getProperty("urls"));
        NSQConfig.setConfigAgentEnv(props.getProperty("configAgentEnv"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));

        config.setLookupAddresses("http://shouldNotBeUsed:4161");

        LookupAddressUpdate lookupUpdate = new LookupAddressUpdate(config);
        String[] lookupAddr = lookupUpdate.getNewLookupAddress();
        Assert.assertEquals(lookupAddr[0], "http://sqs-qa.s.qima-inc.com:4161");

        //hack with reflection to check if lastUpdateTimestamp changes
        Field privateLongField = LookupAddressUpdate.class.
                getDeclaredField("lastUpdateTimestamp");

        privateLongField.setAccessible(true);
        Timestamp timestamp = (Timestamp) privateLongField.get(lookupUpdate);
        Assert.assertNotEquals(timestamp.getTime(), 0L);
    }

    @Test
    /**
     * update lookup address in dcc, sdk need to catch the new lookup addresses
     */
    public void testLookupAddressChange() throws IllegalAccessException, ConfigParserException, IOException, InterruptedException, InvalidConfigException {
        //update dcc properties
        NSQConfig.setUrls(props.getProperty("urls"));
        NSQConfig.setConfigAgentEnv(props.getProperty("configAgentEnv"));
        NSQConfig.setConfigAgentBackupPath(props.getProperty("backupFilePath"));

        config.setLookupAddresses("http://shouldNotBeUsed:4161");

        LookupAddressUpdate lookupUpdate = new LookupAddressUpdate(config);
        String[] lookupAddr = lookupUpdate.getNewLookupAddress();
        Assert.assertEquals(lookupAddr[0], "http://sqs-qa.s.qima-inc.com:4161");

        //update kookup address in dcc
        CountDownLatch latch = publishLookupAddress(props.getProperty("nsq.app.val"), props.getProperty("nsq.lookupd.addr.key"), props.getProperty("nsq.dcc.env"),
                new String[]{"http://sqs-qa.s.qima-inc.com:4161", "http://sqs-qa.s.qima-inc.com:4161", "http://sqs-qa.s.qima-inc.com:4161"});

        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

        logger.info("Sleep for 5 seconds to wait for lookup update...");
        Thread.sleep(1000L);
        logger.info("Main thread awake.");
        lookupAddr = lookupUpdate.getNewLookupAddress();
        Assert.assertEquals(lookupAddr.length, 3);

        //delete addr1 and addr2 again
        latch =  deleteLookupAddress(props.getProperty("nsq.app.val"), props.getProperty("nsq.lookupd.addr.key"), props.getProperty("nsq.dcc.env"), new String[]{"addr1", "addr2"});
        Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

        logger.info("Sleep for 5 seconds to wait for lookup update...");
        Thread.sleep(1000L);
        logger.info("Main thread awake.");
        lookupAddr = lookupUpdate.getNewLookupAddress();
        Assert.assertEquals(lookupAddr.length, 1);
    }
}
