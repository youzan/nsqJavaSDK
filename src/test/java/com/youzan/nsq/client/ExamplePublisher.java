package com.youzan.nsq.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.remoting.connector.ProducerConnector;

/**
 * Created by pepper on 14/12/22.
 */
public class ExamplePublisher {
    private static final Logger logger = LoggerFactory.getLogger(ExamplePublisher.class);

    public static void main(String[] args) {
        try {
            String lookupd = "127.0.0.1";
            int port = 4261;
            ProducerConnector pub = new ProducerConnector(lookupd, port);
            pub.connect();

            String topic = "zhaoxi_test";
            String message = "{'customer_id':'10000353', 'buyer_id':'0', 'kdt_id':'1', 'pay_time':'1432374788'}";
            for (int i = 0; i < 1; i++) {
                System.out.println(pub.put(topic, message));
            }
            pub.close();
            System.out.println("Producer is already closed.");
        } catch (Exception e) {
            logger.error("", e);
        }
        System.out.println("Producer is already closed all.");
        System.exit(0);
    }
}
