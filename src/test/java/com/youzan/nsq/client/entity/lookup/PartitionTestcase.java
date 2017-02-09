package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.entity.IPartitionsSelector;
import com.youzan.nsq.client.entity.Partitions;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.exception.NSQPartitionNotAvailableException;
import com.youzan.nsq.client.exception.NSQProducerNotFoundException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;
import org.easymock.EasyMockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.easymock.EasyMock.expect;

/**
 * Created by lin on 17/1/16.
 */
public class PartitionTestcase extends EasyMockSupport{
    private static final Logger logger = LoggerFactory.getLogger(PartitionTestcase.class);

    private Properties props = new Properties();

    @BeforeClass
    public void init() throws IOException {
        logger.info("At {} , initialize: {}", System.currentTimeMillis(), this.getClass().getName());
        try (final InputStream is = getClass().getClassLoader().getResourceAsStream("app-test.properties")) {
            props.load(is);
        }
        final String env = props.getProperty("env");
    }

    /**
     * trying fetching a missing partition using shardingID
     */
    @Test(expectedExceptions = {NSQPartitionNotAvailableException.class})
    public void testPartitionMissing() throws NSQProducerNotFoundException, NSQTopicNotFoundException, NSQLookupException, NSQPartitionNotAvailableException {
        try {
            Topic mockTopic = partialMockBuilder(Topic.class).withConstructor("java_test_ordered_multi_topic")
                    .addMockedMethod("updatePartitionIndex").createMock();
            expect(mockTopic.updatePartitionIndex(9L, 10)).andStubReturn(9);
            replayAll();
            //and hack partition num
            String cluster = props.getProperty("lookup-addresses");
            List<String> clusters = new ArrayList<>();
            clusters.add(cluster);
            NSQLookupdAddresses lookupd = createNSQLookupdAddr(clusters, clusters);
            IPartitionsSelector ps = lookupd.lookup(mockTopic, true);
            Partitions[] par = ps.choosePartitions();
            //hack partition number
            par[0].updatePartitionNum(10);
            mockTopic.setPartitionID(9);
            par[0].getPartitionAddress(mockTopic.getPartitionId());
        }finally {
            resetAll();
            logger.info("Mocking reset.");
        }
    }

    public NSQLookupdAddresses createNSQLookupdAddr(List<String> clusterIds, List<String> lookupAddrs){
        return NSQLookupdAddresses.create(clusterIds, lookupAddrs);
    }
}
