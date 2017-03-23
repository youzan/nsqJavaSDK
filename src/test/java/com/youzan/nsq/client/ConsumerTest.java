package com.youzan.nsq.client;

/**
 * Created by lin on 17/3/23.
 */
public class ConsumerTest extends AbstractNSQClientTestcase {

//    @Test
//    public void testConsumeWDummyLookupSource() throws NSQException, InterruptedException {
//        NSQConfig config = new NSQConfig("BaseConsumer");
//        config.setLookupAddresses("dcc://123.123.123.123:8089?env=qa");
//        final CountDownLatch latch = new CountDownLatch(1);
//        Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
//            @Override
//            public void process(NSQMessage message) {
//                //do nothing
//            }
//        });
//        consumer.subscribe("JavaTesting-Producer-Base");
//        consumer.start();
//        latch.await();
//    }
}
