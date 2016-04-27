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
        msg = "demo1_test_timestamp_" + System.currentTimeMillis();
        ProducerConnector demo1 = new ProducerConnector(host, port);

        try {
            for (int i = 0; i < 10; i++) {
                demo1.connect();
                demo1.put(topic, msg);
            }
            IOUtil.closeQuietly(demo1);
        } catch (NSQException e) {
            logger.error("Exception", e);
        } catch (InterruptedException e) {
            logger.error("Exception", e);
        } finally {

        }

        host = "127.0.0.1";
        port = 4261;
        topic = "zhaoxi_test";
        msg = "demo2_test_timestamp_" + System.currentTimeMillis();
        ProducerConnector demo2 = new ProducerConnector(host, port);

        try {
            for (int i = 0; i < 10; i++) {
                demo2.connect();
                demo2.put(topic, msg);
            }
            IOUtil.closeQuietly(demo2);
        } catch (NSQException e) {
            logger.error("Exception", e);
        } catch (InterruptedException e) {
            logger.error("Exception", e);
        } finally {

        }

        System.out.println("Done!");
        // System.exit(0);
    }
}
