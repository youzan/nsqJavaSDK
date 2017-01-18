package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.*;
import com.youzan.nsq.client.core.LookupAddressUpdate;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/12/16.
 */
public class ITConsumerOrderPartition {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ITConsumerOrderPartition.class);

    protected final static String controlCnfStr = "{" +
            "\"previous\":[]," +
            "\"current\":[\"http://sqs-qa.s.qima-inc.com:4161\"]," +
            "\"gradation\":{" +
            "\"*\":{\"percent\":10.0}," +
            "\"bc-pifa0\":{\"percent\":10.0}," +
            "\"bc-pifa1\":{\"percent\":20.0}," +
            "\"bc-pifa2\":{\"percent\":30.0}" +
            "}" +
            "}";

    private ConfigAccessAgent agent;
    private Properties props = new Properties();

    @BeforeClass
    public void init() throws IOException {
        logger.info("init of [ITConsumerOrderPartition].");
        logger.info("Initialize ConfigAccessAgent from system specified config.");
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        //load properties from configClientTest.properties
        InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties");
        Properties proTest = new Properties();
        proTest.load(is);
        is.close();
        //initialize system property form config client properties
        logger.info("init of [ITConsumerOrderPartition] ends.");
    }

    @Test
    public void testConsume2SQSWithPartitions() throws NSQException, InterruptedException, ConfigAccessAgentException {
        //note: topic "java_test_ordered_,ulti_topic" has partition num = 9.
        //initialize confgi access agent
        agent = ConfigAccessAgent.getInstance();
        final SortedMap<String, String> valueMap = new TreeMap<>();
        valueMap.put("java_test_ordered_multi_topic", controlCnfStr);
        Topic topic = new Topic("java_test_ordered_multi_topic");

        DCCMigrationConfigAccessDomain domain = (DCCMigrationConfigAccessDomain) DCCMigrationConfigAccessDomain.getInstance(topic);
        Role aRole = Role.getInstance("producer");
        DCCMigrationConfigAccessKey keyProducer = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRole);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyProducer}, valueMap, true);

        Role aRoleConsumer = Role.getInstance("consumer");
        DCCMigrationConfigAccessKey keyConsumer = (DCCMigrationConfigAccessKey) DCCMigrationConfigAccessKey.getInstance(aRoleConsumer);
        TestConfigAccessAgent.updateValue(domain, new AbstractConfigAccessKey[]{keyConsumer}, valueMap, true);

        NSQConfig config =  new NSQConfig("BaseConsumer");
        config.setOrdered(true);
        Producer producer = new ProducerImplV2(config);
        producer.start();
        Message msg;
        final int partitionNum = 9;
        for(int i = 0; i < 1000; i++) {
            String msgStr = "Message " + i%partitionNum + " " + i;
            msg = Message.create(topic, msgStr);
            msg.setTopicShardingID(i%partitionNum);
            producer.publish(msg);
        }
        producer.close();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicInteger cnt = new AtomicInteger(1000);
        final List<List<Integer>> numberLists = new ArrayList<>(9);
        final List<Consumer> consumers = new ArrayList<>();
        for(int i = 0; i < partitionNum; i++) {
            final int idx = i;
            numberLists.add(new ArrayList<Integer>());
            Topic aTopic = new Topic("java_test_ordered_multi_topic");
            aTopic.setPartitionID(idx);
            Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
                @Override
                public void process(NSQMessage message) {
                    logger.info(message.getReadableContent());
                    String[] numbers = message.getReadableContent().split(" ", 3);
                    int partitionId = Integer.valueOf(numbers[1]);
                    int messageNum = Integer.valueOf(numbers[2]);
                    Assert.assertEquals(partitionId, idx);
                    Assert.assertEquals(messageNum%partitionNum, idx);
                    //add to number list
                    List numberList = numberLists.get(idx);
                    synchronized (numberList) {
                        numberList.add(messageNum);
                    }
                    cnt.decrementAndGet();
                    if(cnt.get() == 0)
                        latch.countDown();
                }
            });
            consumer.subscribe(aTopic);
            consumer.start();
            consumers.add(consumer);
        }
        long start  = System.currentTimeMillis();
        latch.await(3, TimeUnit.MINUTES);
        logger.info("It takes {} to wait for consumers.", System.currentTimeMillis() - start);
        Assert.assertEquals(cnt.get(), 0);
        for(int i = 0; i < numberLists.size(); i++) {
           for(int j=1; j < numberLists.get(i).size(); j++){
               Assert.assertTrue(numberLists.get(i).get(j-1) < numberLists.get(i).get(j));
           }
        }
        for(Consumer consumer:consumers)
            consumer.close();
    }

    @AfterMethod
    public void release() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(agent);
    }
}
