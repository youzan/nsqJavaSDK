package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/11/15.
 */
public class ITMigrationTestcase {
    private static final Logger logger = LoggerFactory.getLogger(ITMigrationTestcase.class);
    private static final Properties props = new Properties();

    //old config points to nsq cluster, for producer only
    private NSQConfig oldConfig;
    //new config points to sqs cluster, for producer and consumer
    private NSQConfig newConfig;

    @BeforeClass
    public void init() throws IOException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

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

        newConfig = new NSQConfig(newLookupAddresses, "BaseConsumer");
        newConfig.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        newConfig.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        newConfig.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));
    }

    @Test
    public void test() throws NSQException, InterruptedException {
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
//        CountDownLatch waitLatch = new CountDownLatch(1);
//        waitLatch.await(2, TimeUnit.MINUTES);

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
    }
}
