package com.youzan.nsq.client;

import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Created by lin on 16/9/24.
 */
public class PartitionTestcase extends AbstractNSQClientTestcase{

    private static final Logger logger = LoggerFactory.getLogger(PartitionTestcase.class);

    @Test
    /**
     * subscriber subscribe to one $topic + partition1
     * then change to $topic + partition2
     *
     */
    public void testSubscribeTwoPartitions() throws NSQException, InterruptedException {
//        final AtomicInteger cnt0 = new AtomicInteger(0);
//        final CountDownLatch latch0 = new CountDownLatch(10);
//        Topic topic0 = new Topic("JavaTesting-Partition");
//
//        final AtomicInteger cnt1 = new AtomicInteger(0);
//        final CountDownLatch latch1 = new CountDownLatch(10);
//        Topic topic1 = new Topic("JavaTesting-Partition");
//
//        final String msgPartition0 = "message for partition0";
//        byte[] msg0 = msgPartition0.getBytes();
//        final String msgPartition1 = "message for partition1";
//        byte[] msg1 = msgPartition1.getBytes();
//
//        getNSQConfig().setConsumerName("PartitionConsumer");
//        Consumer consumer0 = PartitionTestcase.createConsumer(this.getNSQConfig(), new MessageHandler() {
//            @Override
//            public void process(NSQMessage message) {
//                logger.info("Message from partition 0");
//                cnt0.incrementAndGet();
//                String msg = new String(message.getMessageBody());
//                logger.info("#{}: {}", cnt0, msg);
//                Assert.assertEquals(msg, msgPartition0);
//                latch0.countDown();
//            }
//        });
//
//        consumer0.subscribe(topic0.getPartitionId(), topic0.getTopicText());
//        consumer0.start();
//
//        Consumer consumer1 = PartitionTestcase.createConsumer(this.getNSQConfig(), new MessageHandler() {
//            @Override
//            public void process(NSQMessage message) {
//                logger.info("Message from partition 1");
//                cnt1.incrementAndGet();
//                String msg = new String(message.getMessageBody());
//                logger.info("#{}: {}", cnt1, msg);
//                Assert.assertEquals(msg, msgPartition1);
//                latch1.countDown();
//            }
//        });
//        consumer1.subscribe(topic1.getPartitionId(), topic1.getTopicText());
//        consumer1.start();
//
//        Producer producer = PartitionTestcase.createProducer(getNSQConfig());
//        producer.start();
//
//        for(int i = 0; i < 10; i++) {
//            producer.publish(msg0, topic0);
//            producer.publish(msg1, topic1);
//        }
//        producer.close();
//
//        Assert.assertTrue(latch0.await(1, TimeUnit.MINUTES));
//        Assert.assertTrue(latch1.await(1, TimeUnit.MINUTES));
    }
}
