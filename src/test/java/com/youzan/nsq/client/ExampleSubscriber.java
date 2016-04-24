package com.youzan.nsq.client;

import com.youzan.nsq.client.remoting.connector.CustomerConnector;
import com.youzan.nsq.client.remoting.listener.ConnectorListener;
import com.youzan.nsq.client.remoting.listener.NSQEvent;

/**
 * Created by pepper on 14/12/22.
 */
public class ExampleSubscriber {

    public static void main(String[] args) throws Throwable {
        start("binlog_order_paysuccess");

    }

    public static void start(String topic) throws Throwable {
        ConnectorListener listener = new ConnectorListener() {
            @Override
            public void handleEvent(NSQEvent event) throws Exception {
                if (NSQEvent.READ.equals(event.getType())) {
                    System.out.println(event.getMessage());
                }
            }
        };

        CustomerConnector connector = new CustomerConnector("192.168.66.202,192.168.66.202", 4161, topic, "default");
        connector.setSubListener(listener);
        connector.connect();

    }
}
