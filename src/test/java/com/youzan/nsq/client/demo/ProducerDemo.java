/**
 * 
 */
package com.youzan.nsq.client.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.remoting.connector.ProducerConnector;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class ProducerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    /**
     * @param args
     */
    public static void main(String[] args) {
        String host, topic, msg;
        int port;

        host = "127.0.0.1";
        port = 4161;
        topic = "zhaoxi_test";

        @SuppressWarnings("resource")
        ProducerConnector demo1 = new ProducerConnector(host, port);

        host = "127.0.0.1";
        port = 4261;
        topic = "zhaoxi_test";

        @SuppressWarnings("resource")
        ProducerConnector demo2 = new ProducerConnector(host, port);
        demo1.connect();
        demo2.connect();

        for (;;) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.interrupted();
            }

            try {
                msg = "demo1_test_timestamp_" + System.currentTimeMillis();
                demo1.put(topic, msg);

                msg = "demo2_test_timestamp_" + System.currentTimeMillis();
                demo2.put(topic, msg);

            } catch (NSQException e) {
                logger.error("Exception", e);
            } catch (InterruptedException e) {
                logger.error("Exception", e);
            } catch (Exception e) {
                logger.error("Exception", e);
            } finally {
            }
        }
        // System.exit(0);
    }
}
