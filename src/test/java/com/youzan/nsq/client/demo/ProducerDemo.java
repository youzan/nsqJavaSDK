/**
 * 
 */
package com.youzan.nsq.client.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.remoting.connector.ProducerConnector;
import com.youzan.util.IOUtil;

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

        ProducerConnector demo1 = new ProducerConnector(host, port);

        host = "127.0.0.1";
        port = 4261;
        topic = "zhaoxi_test";

        ProducerConnector demo2 = new ProducerConnector(host, port);
        try {
            demo1.connect();
            demo2.connect();
            for (;;) {
                msg = "demo1_test_timestamp_" + System.currentTimeMillis();
                demo1.put(topic, msg);

                msg = "demo2_test_timestamp_" + System.currentTimeMillis();
                demo2.put(topic, msg);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        } catch (NSQException e) {
            logger.error("Exception", e);
        } catch (InterruptedException e) {
            logger.error("Exception", e);
        } finally {
            IOUtil.closeQuietly(demo1, demo2);
        }
        System.out.println("Done!");
        // System.exit(0);
    }
}
