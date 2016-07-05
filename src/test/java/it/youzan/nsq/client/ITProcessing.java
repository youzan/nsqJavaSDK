package it.youzan.nsq.client;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.ProducerImplV2;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.exception.NSQException;

public class ITProcessing {

    private static final Logger logger = LoggerFactory.getLogger(ITProcessing.class);

    private final Random random = new Random();
    private NSQConfig config;

    @BeforeClass
    public void init() throws NSQException {
        config = new NSQConfig();
        final Properties props = new Properties();
        // load
        props.clear();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void process() {
        // Generate some messages
        // Publish to the data-node
        // Get the messsages
        // Validate the messages
        final NSQMessage[] incommings = new NSQMessage[2];
        final AtomicInteger index = new AtomicInteger(0);
        final MessageHandler handler = new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                incommings[index.getAndIncrement()] = message;
            }
        };
        final Consumer consumer = new ConsumerImplV2(config, handler);
        /****************************** string type ***************************/
        final String message1 = MessageUtil.randomString();
        try (Producer p = new ProducerImplV2(config)) {
            p.publish(message1);
        } catch (NSQException e) {
            logger.error("Exception", e);
        }
        /****************************** byte type *****************************/
        sleep(1);
        final byte[] message2 = new byte[1024];
        random.nextBytes(message2);
        try (Producer p = new ProducerImplV2(config);) {
            p.publish(message2);
        } catch (NSQException e) {
            logger.error("Exception", e);
        }

        for (NSQMessage msg : incommings) {
            Assert.assertEquals(msg.getReadableAttempts(), 1,
                    "The message is not mine. Please check the data in the environment.");
        }
        Assert.assertEquals(incommings[0].getReadableContent(), message1);
        Assert.assertEquals(incommings[1].getMessageBody(), message2);

        // Because of the
        sleep(10);
        consumer.close();
    }

    /**
     * @param millisecond
     */
    private void sleep(final long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }
}
