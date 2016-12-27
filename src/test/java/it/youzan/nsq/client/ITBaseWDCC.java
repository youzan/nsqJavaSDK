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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ITBaseWDCC {
    private final static Logger logger = LoggerFactory.getLogger(ITBaseWDCC.class);


    @BeforeMethod
    public void init() {
        /*
         * 配置全局NSQ配置变量,此处展示通过代码配置的一部分,
         * 1. 使用{@link NSQConfig#setSDKEnv(String)} 配置当前的环境,如: qa, prod;
         * 2. 指定Config access remote的地址,此处指定DCC的地址;
         * note: 必须在实例化NSQ Client之前设置全局变量。
         */
        ConfigAccessAgent.setEnv("prod");
        ConfigAccessAgent.setConfigAccessRemotes("http://10.9.7.75:8089");
    }

    @Test
    public void test() throws NSQException, InterruptedException {
        //实例化producer端的配置对象
        NSQConfig configProducer = new NSQConfig();
        configProducer
                .setUserSpecifiedLookupAddress(true)
                .setLookupAddresses("http://sqs-qa.s.qima-inc.com:4161")
                .setConnectTimeoutInMillisecond(500)
                .setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors())
                .setRdy(1);

        //创建topic, 用于发送消息
        Topic aTopic = new Topic("JavaTesting-Producer-Base");

        //实例化producer
        Producer producer = new ProducerImplV2(configProducer);
        //启动producer
        producer.start();
        for(int i = 0; i < 100; i++) {
            //要发送的消息内容
            String msgStr = "Message " + i;

            //方法1. 发送一条消息
            producer.publish(msgStr.getBytes(Charset.defaultCharset()), aTopic);
            //方法2. 发送一条消息
            Message msg = Message.create(aTopic, msgStr);
            producer.publish(msg);
            //方法3. 发送一条消息
            producer.publish(msgStr.getBytes(Charset.defaultCharset()), "JavaTesting-Producer-Base");
        }

        //关闭Producer
        producer.close();

        final CountDownLatch latch = new CountDownLatch(1);
        //实例化consumer端的配置对象,使用带有channel name的构造函数
        NSQConfig configConsume = new NSQConfig("BaseConsumer");
        //connection pool size in Producer size and event pool size in Conusmer size
        configConsume
                .setUserSpecifiedLookupAddress(true)
                .setLookupAddresses("http://sqs-qa.s.qima-inc.com:4161")
                //TCP 链接超时
                .setConnectTimeoutInMillisecond(500)
                //thread io size for nio event pool
                .setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors())
                //设置Rdy大小
                .setRdy(1);

        final AtomicInteger cnt = new AtomicInteger(0);
        Consumer consumer = new ConsumerImplV2(configConsume, new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                //在此定义用户的消息处理逻辑
                int count = cnt.incrementAndGet();
                if(300 == count)
                    latch.countDown();
            }
        });
        //auto finish,默认开启
        //consumer.setAutoFinish(true);
        //创建topic消费
        consumer.subscribe(aTopic);
        //启动consumer
        consumer.start();
        //other logic....
        latch.await(2, TimeUnit.MINUTES);
        //关闭 consumer
        consumer.close();
        Assert.assertEquals(cnt.get(), 300);
        logger.info("Received {} message(s).", cnt.get());
    }



    @AfterMethod
    public void release() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        System.clearProperty("nsq.sdk.configFilePath");
        Method method = ConfigAccessAgent.class.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(ConfigAccessAgent.getInstance());
    }
}
