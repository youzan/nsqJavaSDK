package com.youzan.nsq.client.core.topics;

import com.youzan.nsq.client.AbstractNSQClientTestcase;
import com.youzan.nsq.client.Consumer;
import com.youzan.nsq.client.ConsumerImplV2;
import com.youzan.nsq.client.MessageHandler;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.SortedSet;

/**
 * Created by lin on 16/9/27.
 */
public class TopicsTestCase extends AbstractNSQClientTestcase{
    private static final Logger logger = LoggerFactory.getLogger(TopicsTestCase.class);

    @BeforeClass
    public void init() throws IOException {
        super.init();
    }

    @Test
    public void testTopicSubscribe() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String topicTest = "notopic";
        Consumer consumer = createConsumer(getNSQConfig(), new MessageHandler() {
            @Override
            public void process(NSQMessage message) {
                //do nothing
            }
        });

        //subscribe a partition id no specified topic, then subscribe same topics with 5 id, no partition specified topic
        //should be deleted.
        consumer.subscribe(topicTest);
        for(int parID = 0; parID < 5; parID++)
            consumer.subscribe(parID, topicTest);

        //comes to verified, hack
        Class concumserClz = ConsumerImplV2.class;
        Method getTopicsMethod = concumserClz.getDeclaredMethod("getTopics");
        getTopicsMethod.setAccessible(true);
        SortedSet<Topic> topics = (SortedSet<Topic>) getTopicsMethod.invoke(consumer);
        for(Topic topic : topics){
            logger.info(topic.toString());
        }
        Assert.assertEquals(topics.size(), 5);

        consumer.subscribe(topicTest);
        for(Topic topic : topics){
            logger.info(topic.toString());
        }
        Assert.assertEquals(topics.size(), 1);

        String topicTest2 = "notopic2";
        for(int parID = 1; parID <= 5; parID++) {
            consumer.subscribe(parID, topicTest2);
            consumer.subscribe(parID, topicTest);
        }
        Assert.assertEquals(10, topics.size());
        consumer.subscribe(6, topicTest);
        //assert that new subscribed topic in idx#5
        int idx = 0;
        Topic topicInsert = new Topic(topicTest, 6);
        for(Topic topic : topics){
            if(topic.equals(topicInsert)) {
                Assert.assertEquals(5, idx);
                break;
            }
            idx++;
        }

        consumer.subscribe(topicTest);
        Assert.assertEquals(topics.size(), 6);
    }

    @AfterMethod
    public void release() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Class<ConfigAccessAgent> clazz = ConfigAccessAgent.class;
        Method method = clazz.getDeclaredMethod("release");
        method.setAccessible(true);
        method.invoke(ConfigAccessAgent.getInstance());
    }
}
