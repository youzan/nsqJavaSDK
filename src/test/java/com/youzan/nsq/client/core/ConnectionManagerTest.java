package com.youzan.nsq.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.nsq.client.IConsumeInfo;
import com.youzan.nsq.client.MockedNSQConnectionImpl;
import com.youzan.nsq.client.MockedNSQSimpleClient;
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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by lin on 17/6/26.
 */
public class ConnectionManagerTest {

    private final Logger logger = LoggerFactory.getLogger(ConnectionManagerTest.class.getName());
    private Properties props = new Properties();
    private NSQConfig config = new NSQConfig("BaseConsumer");
    private String lookupAddr;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;

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

    @Test
    public void testBackoff() throws IOException, InterruptedException {
        backoff();
    }


    private ConnectionManager backoff() throws IOException, InterruptedException {
        NSQConfig config = (NSQConfig) this.config.clone();
        String topicName = "JavaTesting-Producer-Base";
        JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicName + "&access=r"));
        JsonNode partition = lookupResp.get("partitions").get("0");
        Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicName, 0, false);
        NSQConnection con1 = connect(addr1, topicName, 0, "BaseConsumer", config);

        ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
            @Override
            public float getLoadFactor() {
                return 0;
            }

            @Override
            public int getRdyPerConnection() {
                return 0;
            }

            @Override
            public boolean isConsumptionEstimateElapseTimeout() {
                return false;
            }
        });
        conMgr.subscribe(topicName, con1);
        conMgr.backoff(topicName);
        Thread.sleep(100);

        assert 0 == con1.getCurrentRdyCount();
        return conMgr;
    }

    @Test
    public void testResume() throws IOException, InterruptedException {
        ConnectionManager conMgr = backoff();
        conMgr.resume("JavaTesting-Producer-Base");
        Thread.sleep(100);

        assert 1 == conMgr.getSubscribeConnections("JavaTesting-Producer-Base")
                .iterator()
                .next()
                .getConn()
                .getCurrentRdyCount();
    }

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
        con1.setTopic(new Topic(topic, partition));
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

    @Test
    public void testRdyDecline() throws IOException, InterruptedException {
        logger.info("[testRdyDecline] starts.");
        try {
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(5);
            ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    //water high
                    return 2;
                }

                @Override
                public int getRdyPerConnection() {
                    return 6;
                }

                @Override
                public boolean isConsumptionEstimateElapseTimeout() {
                    return true;
                }
            });

            int partitionNum = 5;
            String topic = "test5Par1Rep";
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con, 5);
                connList.add(con);
            }

            conMgr.start(0);
            logger.info("Sleep 30sec to wait for rdy declining");
            Thread.sleep(30000);

            for (NSQConnection conn : connList) {
                Assert.assertEquals(conn.getCurrentRdyCount(), 1);
            }
        } finally {
            logger.info("[testRdyDecline] ends.");
        }
    }

    @Test
    public void testRdyIncrease() throws IOException, InterruptedException {
        logger.info("[testRdyIncrease] starts.");
        try {
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(5);
            ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0.5f;
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

            int partitionNum = 5;
            String topic = "test5Par1Rep";
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }

            conMgr.start(0);
            logger.info("Sleep 30sec to wait for rdy increasing");
            Thread.sleep(30000);

            for (NSQConnection conn : connList) {
                Assert.assertEquals(conn.getCurrentRdyCount(), 5);
            }
        } finally {
            logger.info("[testRdyIncrease] ends.");
        }
    }

    @Test
    public void testExpectedRdy() throws IOException, InterruptedException {
        logger.info("[testExpectedRdy] starts.");
        try {
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(6);
            ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
                @Override
                public float getLoadFactor() {
                    return 0.5f;
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

            int partitionNum = 5;
            String topic = "test5Par1Rep";
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(partitionNum);
            for (int i = 0; i < partitionNum; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, 0, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }
            //pick 2 connection and fix another rdy
            connList.get(0).declineExpectedRdy();
            connList.get(0).declineExpectedRdy();
            connList.get(0).declineExpectedRdy();

            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();
            connList.get(1).declineExpectedRdy();

            conMgr.start(0);
            Thread.sleep(60000);

            Assert.assertEquals(connList.get(0).getCurrentRdyCount(), 3);
            Assert.assertEquals(connList.get(1).getCurrentRdyCount(), 2);
        } finally {
            logger.info("[testExpectedRdy] ends.");
        }
    }


    @Test
    public void testRemoveConnectionWrapper() throws IOException, InterruptedException {
        logger.info("[testRemoveConnectionWrapper] starts.");
        try {
            NSQConfig config = (NSQConfig) this.config.clone();
            config.setRdy(6);
            ConnectionManager conMgr = new ConnectionManager(new IConsumeInfo() {
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

            int par1 = 5;
            String topic = "test5Par1Rep";
            JsonNode lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topic + "&access=r"));
            List<NSQConnection> connList = new ArrayList<>(par1);
            for (int i = 0; i < par1; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topic, i, false);
                NSQConnection con = connect(addr1, topic, i, "BaseConsumer", config);
                conMgr.subscribe(topic, con);
                connList.add(con);
            }

            //pick another topic
            int par2 = 1;
            String topicJ = "JavaTesting-Producer-Base";
            lookupResp = IOUtil.readFromUrl(new URL("http://" + lookupAddr + "/lookup?topic=" + topicJ + "&access=r"));
            for (int i = 0; i < par2; i++) {
                JsonNode partition = lookupResp.get("partitions").get("" + i);
                Address addr1 = new Address(partition.get("broadcast_address").asText(), partition.get("tcp_port").asText(), partition.get("version").asText(), topicJ, i, false);
                NSQConnection con = connect(addr1, topicJ, i, "BaseConsumer", config);
                conMgr.subscribe(topicJ, con);
                connList.add(con);
            }

            //remove par0 par1 for test5Par1Rep
            Map<String, List<Address>> removeMap = new HashMap<>();
            removeMap.put(topic, new ArrayList<Address>());
            List<Address> conLists = removeMap.get(topic);
            conLists.add(connList.get(0).getAddress());
            conLists.add(connList.get(1).getAddress());

            Assert.assertTrue(conMgr.remove(removeMap));

            Set<ConnectionManager.NSQConnectionWrapper> conWprs = conMgr.getSubscribeConnections(topic);
            Assert.assertEquals(conWprs.size(), 3);
            conWprs = conMgr.getSubscribeConnections(topicJ);
            Assert.assertEquals(conWprs.size(), 1);

            conLists.add(connList.get(2).getAddress());

            Assert.assertTrue(conMgr.remove(removeMap));

            conWprs = conMgr.getSubscribeConnections(topic);
            Assert.assertEquals(conWprs.size(), 2);

            //add another topic-> conn into map
            removeMap.put(topicJ, new ArrayList<Address>());
            List<Address> conListsJ = removeMap.get(topicJ);
            conListsJ.add(connList.get(5).getAddress());

            Assert.assertTrue(conMgr.remove(removeMap));

            conWprs = conMgr.getSubscribeConnections(topicJ);
            Assert.assertNull(conWprs);

        } finally {
            logger.info("[testRemoveConnectionWrapper] ends.");
        }
    }
}
