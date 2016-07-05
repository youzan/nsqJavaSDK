package it.youzan.nsq.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.exception.NSQException;

public class ITProducer {

    private static final Logger logger = LoggerFactory.getLogger(ITProducer.class);

    @Test
    public void produceUsingSimpleProducer() throws NSQException {
        // 创建配置: 要连接的集群参数和本机进程参数
        final NSQConfig config = new NSQConfig();
        // 设置Topic Name
        config.setTopic("");
        // 设置Lookupd集群(多)地址, 是以","分隔的字符串,就是说可以配置一个集群里的多个节点
        config.setLookupAddresses("");
        // 设置Netty里的ThreadPoolSize(带默认值): 1Thread-to-1IOThread, 使用BlockingIO
        config.setThreadPoolSize4IO(2);
        // 设置timeout(带默认值): 一次IO来回+本机执行了返回给client code完成的消耗时间
        config.setTimeoutInSecond(3);
        // 设置message中client-server之间可以的timeout(带默认值)
        config.setMsgTimeoutInMillisecond(60 * 1000);
    }

    /**
     * @param millisecond
     */
    private void sleep(final long millisecond) {
        try {
            Thread.sleep(millisecond);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("System is too busy! Please check it!", e);
        }
    }
}
