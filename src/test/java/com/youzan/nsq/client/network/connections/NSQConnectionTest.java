package com.youzan.nsq.client.network.connections;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.nsq.client.IConsumeInfo;
import com.youzan.nsq.client.MockedConsumer;
import com.youzan.nsq.client.MockedNSQConnectionImpl;
import com.youzan.nsq.client.MockedNSQSimpleClient;
import com.youzan.nsq.client.core.*;
import com.youzan.nsq.client.core.command.Identify;
import com.youzan.nsq.client.core.command.Magic;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Role;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.network.netty.NSQClientInitializer;
import com.youzan.util.IOUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by lin on 17/7/25.
 */
public class NSQConnectionTest {

    private final Logger logger = LoggerFactory.getLogger(ConnectionManagerTest.class.getName());
    private Properties props = new Properties();
    private NSQConfig config = new NSQConfig("BaseConsumer");
    private String lookupAddr;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;

    private NSQConnection connect(Address addr, String topic, int partition, String channel, NSQConfig config) throws InterruptedException {
        ChannelFuture chFuture = bootstrap.connect(addr.getHost(), addr.getPort());
        final CountDownLatch connLatch = new CountDownLatch(1);
        chFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess())
                    connLatch.countDown();
            }
        });
        connLatch.await(500, TimeUnit.MILLISECONDS);
        Channel ch = chFuture.channel();
        MockedNSQConnectionImpl con1 = new MockedNSQConnectionImpl(0, addr, ch, config);
        con1.setTopic(topic);
        NSQSimpleClient simpleClient = new MockedNSQSimpleClient(Role.Consumer, false);
        ch.attr(Client.STATE).set(simpleClient);
        ch.attr(NSQConnection.STATE).set(con1);
        con1.command(Magic.getInstance());
        con1.command(new Identify(config));
        Thread.sleep(100);
        con1.command(new Sub(new Topic(topic, partition), channel));
        Thread.sleep(100);
        con1.command(new Rdy(1));
        Thread.sleep(100);

        return con1;
    }

    @BeforeClass
    public void init() throws IOException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        System.setProperty("nsq.sdk.configFilePath", "src/test/resources/configClientTest.properties");
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
        logger.debug("The environment is {} .", env);
        lookupAddr = props.getProperty("lookup-addresses");
        final String connTimeout = props.getProperty("connectTimeoutInMillisecond");
        final String msgTimeoutInMillisecond = props.getProperty("msgTimeoutInMillisecond");
        final String threadPoolSize4IO = props.getProperty("threadPoolSize4IO");

        config = new NSQConfig();
        config.setLookupAddresses(lookupAddr);
        config.setConnectTimeoutInMillisecond(Integer.valueOf(connTimeout));
        config.setMsgTimeoutInMillisecond(Integer.valueOf(msgTimeoutInMillisecond));
        config.setThreadPoolSize4IO(Integer.valueOf(threadPoolSize4IO));


        //netty setup
        this.bootstrap = new Bootstrap();
        this.eventLoopGroup = new NioEventLoopGroup(config.getThreadPoolSize4IO());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutInMillisecond());
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
    }

    @Test(invocationCount = 3)
    public void testDuplicateCloseOnNSQConnection() throws IOException, InterruptedException {
        NSQConfig config = (NSQConfig) this.config.clone();
        String topicName = "JavaTesting-Producer-Base";
        JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r"));
        JsonNode partition = lookupResp.get("partitions").get("0");

        Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, false);

        //case1
        NSQConnection con1 = connect(addr1, topicName, 0, "BaseConsumer", config);
        Thread.sleep(50);
        con1.close();
        con1.close();

        //case2
        final ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 6;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });

        NSQConnection con2 = connect(addr1, topicName, 0, "BaseConsumer", config);
        conMgr.subscribe(topicName, con2);
        MockedConsumer consumer = new MockedConsumer(config, null);
        consumer.start();
        consumer.setConnectionManager(conMgr);
        con2.close();
        consumer.close(con2);


        //case3
        NSQConnection con3 = connect(addr1, topicName, 0, "BaseConsumer", config);
        conMgr.subscribe(topicName, con2);
        consumer.close(con3);
        con3.close();
    }
}
