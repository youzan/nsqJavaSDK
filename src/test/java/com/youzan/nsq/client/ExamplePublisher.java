package com.youzan.nsq.client;

import com.youzan.nsq.client.remoting.connector.ProducerConnector;

/**
 * Created by pepper on 14/12/22.
 */
public class ExamplePublisher {

    public static void main(String[] args) {
        try {
            ProducerConnector pub = new ProducerConnector("192.168.66.202", 4161);
            pub.connect();

            String topic = "binlog_order_paysuccess";
            String message = "{'customer_id':'10000353', 'buyer_id':'0', 'kdt_id':'1', 'pay_time':'1432374788'}";
            for (int i = 0; i < 10; i++) {
                System.out.println(pub.put(topic, message));
            }
            pub.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
