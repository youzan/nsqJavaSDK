/**
 * 
 */
package com.youzan.nsq.client.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.remoting.connector.CustomerConnector;
import com.youzan.nsq.client.remoting.listener.ConnectorListener;
import com.youzan.nsq.client.remoting.listener.NSQEvent;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        ConsumerDemo demo1 = new ConsumerDemo();
        ConsumerDemo demo2 = new ConsumerDemo();

        String host, topic, channel;
        int port = 0;

        host = "127.0.0.1";
        port = 4161;
        topic = "zhaoxi_test";
        channel = "zhaoxi_is_a_consumer";
        demo1.start(host, port, topic, channel);

        host = "127.0.0.1";
        port = 4261;
        topic = "zhaoxi_test";
        channel = "zhaoxi_is_a_consumer";
        demo2.start(host, port, topic, channel);

    }

    void start(String host, int port, String topic, String channel) {
        ConnectorListener listener = new ConnectorListener() {
            @Override
            public void handleEvent(NSQEvent event) throws Exception {
                if (NSQEvent.READ.equals(event.getType())) {
                    System.out.println(event.getMessage());
                }
            }
        };
        CustomerConnector connector = new CustomerConnector(host, port, topic, channel);
        connector.setSubListener(listener);
        connector.connect();
    }
}
