package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.ConfigAccessAgentException;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 16/12/21.
 */
public class ITStableCaseWDCC {
    private static final Logger logger = LoggerFactory.getLogger(ITStableCaseWDCC.class);

    private final String consumerName = "BaseConsumer";

    private boolean stable;
    private long allowedRunDeadline = 0;
    private final Random _r = new Random();
    private final BlockingQueue<NSQMessage> store = new LinkedBlockingQueue<>(1000);

    private AtomicInteger successPub = new AtomicInteger(0);
    private AtomicInteger totalPub = new AtomicInteger(0);

    private AtomicInteger received = new AtomicInteger(0);
    private AtomicInteger successFinish = new AtomicInteger(0);


    private final NSQConfig config = new NSQConfig();
    private Producer producer;
    private Consumer consumer;
    private final String TOPICNAME = "JavaTesting-Migration";

    @BeforeClass
    public void init() throws Exception {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());

        final String stableProp = System.getProperty("stable", "false");
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTestStableWDCC.properties");
        stable = Boolean.valueOf(stableProp);
        if (!stable) {
            return;
        }
        final String hoursProp = System.getProperty("hours", "4");
        allowedRunDeadline = TimeUnit.HOURS.toMillis(Long.valueOf(hoursProp)) + System.currentTimeMillis();
        logger.info("Now {} , allowedRunDeadline {} . Got {} hours", System.currentTimeMillis(), allowedRunDeadline, hoursProp);


        final Properties props = new Properties();
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }

        final String lookups = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
//        config.setUserSpecifiedLookupAddress(true);
//        config.setLookupAddresses(lookups);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setThreadPoolSize4IO(1);
    }

    @Test(priority = 12)
    public void produce() throws NSQException, InterruptedException {
        if (!stable) {
            return;
        }
        logger.info("Stable test producer starts.");
        logger.info(""+allowedRunDeadline);
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setThreadPoolSize4IO(1);
        producer = new ProducerImplV2(config);
        producer.start();
        for (long now = 0; now < allowedRunDeadline; now = System.currentTimeMillis()) {
            logger.info("Producer send at: {}, DeadLine: {}", now, allowedRunDeadline);
            byte[] message = ("Message At: " + now).getBytes();
            try {
                totalPub.getAndIncrement();
                producer.publish(message, TOPICNAME);
                successPub.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
            int sec = _r.nextInt(5) * 100;
            Thread.sleep(sec);
        }
        logger.info("Exit producing...");
    }

    @Test(priority = 12)
    public void consume() throws InterruptedException, NSQException {
        if (!stable) {
            return;
        }
        logger.info("Stable test consumer starts.");
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info("Message received: " + message.getReadableContent());
                received.getAndIncrement();
                store.offer(message);
            }
        };
        final NSQConfig config = (NSQConfig) this.config.clone();
        config.setRdy(4);
        config.setConsumerName(consumerName);
        config.setThreadPoolSize4IO(Math.max(2, Runtime.getRuntime().availableProcessors()));
        consumer = new ConsumerImplV2(config, handler);
        consumer.setAutoFinish(false);
        consumer.subscribe(TOPICNAME);
        consumer.start();

        for (long now = 0; now < (allowedRunDeadline + 10 * 1000); now = System.currentTimeMillis()) {
            try {
                final NSQMessage message = store.poll(2, TimeUnit.SECONDS);
                if (message == null) {
                    continue;
                }
                logger.info("Message got at: {}, Deadline: {}", now, allowedRunDeadline);
                consumer.finish(message);
                successFinish.getAndIncrement();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }
        logger.info("Exit consuming...");
    }


    @AfterClass
    public void close() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ConfigAccessAgentException {
        IOUtil.closeQuietly(consumer, producer);
        logger.info("Done. successPub: {} , totalPub: {} , received: {} , successFinish: {} , now the temporary store in memory has {} messages.", successPub.get(), totalPub.get(), received.get(), successFinish.get(), store.size());
        System.clearProperty("nsq.sdk.configFilePath");
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(ConfigAccessAgent.getInstance());
    }

}
