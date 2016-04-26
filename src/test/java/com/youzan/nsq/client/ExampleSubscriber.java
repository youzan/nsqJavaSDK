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
        while (true) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
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

        String lookupd = "127.0.0.1";
        int port = 4161;
        CustomerConnector connector = new CustomerConnector(lookupd, port, topic, "default");
        connector.setSubListener(listener);
        connector.connect();
    }
}
