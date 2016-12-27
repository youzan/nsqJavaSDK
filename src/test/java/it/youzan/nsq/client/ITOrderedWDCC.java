package it.youzan.nsq.client;

import com.youzan.nsq.client.*;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ITOrderedWDCC {

    private final static Logger logger = LoggerFactory.getLogger(ITOrderedWDCC.class);

    @BeforeClass
    public void init() {
        //指定config access全局配置
        ConfigAccessAgent.setEnv("prod");
        ConfigAccessAgent.setConfigAccessRemotes("http://10.9.7.75:8089");
    }

    @Test
    public void test() throws NSQException, InterruptedException {
        NSQConfig configProduce = new NSQConfig();
        Producer producer  = new ProducerImplV2(configProduce);
        producer.start();

        //设置sharding ID, sdk通过shardingID映射到指定的消息分区,
        long shardingId = 0;
        Topic aTopic = new Topic("JavaTesting-Order");
        for(int i = 0; i < 200; i++){
            String msgStr = "msg: " + i;
            Message msg = Message.create(aTopic, msgStr);
            //在Message对象中指定shardingID, 默认sharding策略将shardingID与topic的partition number取模的结果作为目标的partition ID
            //shardingID为0
            msg.setTopicShardingID(shardingId);
            //发送该消息
            producer.publish(msg);
        }

        shardingId = 1;
        for(int i = 0; i < 200; i++){
            String msgStr = "msg: " + i;
            Message msg = Message.create(aTopic, msgStr);
            //在Message对象中指定shardingID, 默认sharding策略将shardingID与topic的partition number取模的结果作为目标的partition ID
            //shardingID为1
            msg.setTopicShardingID(shardingId);
            //发送该消息至
            producer.publish(msg);
        }

        //关闭producer
        producer.close();
        NSQConfig configConsume = new NSQConfig("BaseConsumer");
        //打开SubOrder标志位
        configConsume.setOrdered(true);
        aTopic = new Topic("JavaTesting-Order");
        final CountDownLatch consumeLatch = new CountDownLatch(400);
        Consumer consumer = new ConsumerImplV2(configConsume, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                logger.info(message.getReadableContent());
                consumeLatch.countDown();
            }
        });
        //指定消费的partition ID,默认情况下不指定,将消费topic下全部可用分区
        //aTopic.setPartitionID(1);
        consumer.subscribe(aTopic);
        consumer.start();
        Assert.assertTrue(consumeLatch.await(3, TimeUnit.MINUTES));
        consumer.close();
    }

    @AfterClass
    public void release() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(ConfigAccessAgent.getInstance());
    }
}
