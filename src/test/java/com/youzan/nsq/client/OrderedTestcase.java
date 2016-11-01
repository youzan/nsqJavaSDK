package com.youzan.nsq.client;

import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.entity.Message;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * pub&sub Order testcase as Pub
 * Created by lin on 16/9/26.
 */
public class OrderedTestcase extends AbstractNSQClientTestcase{

    private static final Logger logger = LoggerFactory.getLogger(OrderedTestcase.class);
    private final Topic topic = new Topic("JavaTesting-Order");
    private final int msgNum = 2;

    @BeforeClass
    public void init() throws IOException {
        super.init();
        //turn on ordered
        getNSQConfig().setOrdered(true);
    }

    @Test(priority = 1)
    public void consume() throws NSQException, InterruptedException {


        //set a channel
        getNSQConfig().setConsumerName("OrderConsumer");
        final CountDownLatch latch = new CountDownLatch(msgNum);
        final Consumer consumer = createConsumer(getNSQConfig(), new MessageHandler() {
            private AtomicLong cnt = new AtomicLong(0);

            @Override
            public void process(NSQMessage message) {
                logger.info("Message #{} received: {}", cnt.incrementAndGet(), message.getReadableContent());
                latch.countDown();
            }
        });
        consumer.subscribe(1, topic.getTopicText());
        consumer.start();
        long start = System.currentTimeMillis();
        latch.await();
        logger.info("Takes {} secs to finish consuming.", (System.currentTimeMillis() - start) * 0.001);
        consumer.close();
    }

    @Test
    public void publish() throws NSQException, IllegalAccessException, ConfigParserException, IOException {
        publishTraceConfig("[{\"key\":\"TestTrace1\",\"value\":\"true\"},{\"key\":\"TestTrace2\",\"value\":\"false\"},{\"key\":\"JavaTesting-Order\",\"value\":\"true\"}]");

        Producer producer = createProducer(getNSQConfig());
        producer.start();
        for(int i = 0; i < msgNum; i++) {
            Message msg = Message.create(topic, 1024L, ("Message #" + i));
            producer.publish(msg);
        }
        producer.close();
    }

    private CountDownLatch publishTraceConfig(String traceContent) throws IOException, IllegalAccessException, ConfigParserException {
        //create publisher to configs remote to upload combination config
        ConfigClient client = ConfigClientBuilder.create()
                .setRemoteUrls(props.getProperty("urls").split(","))
                .setClientConfig(new ClientConfig())
                .setBackupFilePath(props.getProperty("backupFilePath"))
                .setConfigEnvironment(props.getProperty("configAgentEnv"))
                .build();
        //config for trace
        Config traceConfig = (Config) Config.create()
                .setApp("nsq")
                .setKey("trace")
                .setEnv(props.getProperty("configAgentEnv"))
                .setValue(Config.ConfigType.COMBINATION, traceContent)
                .build();
        List<Config> configs = new ArrayList<>();
        configs.add(traceConfig);
        final CountDownLatch latch = new CountDownLatch(1);
        client.publish(new IResponseCallback() {
            @Override
            public void onChanged(List<Config> list) throws Exception {
                logger.info("Publish successfully.");
                latch.countDown();
            }

            @Override
            public void onFailed(List<Config> list, Exception e) throws Exception {
                logger.error("Publish to configs failed. ", e);
                Assert.fail("Publish to configs failed");
            }
        }, configs);

        return latch;
    }


}
