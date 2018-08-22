package com.youzan.nsq.client.metrics;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.NSQMessage;
import org.easymock.EasyMockSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

import static org.easymock.EasyMock.expect;

/**
 * Created by lin on 18/8/20.
 */
public class MessageCounterCallbackTest extends EasyMockSupport {

    @Test
    public void testCounterDelta() {
        NSQConfig cfg = new NSQConfig("BaseConsumer");
        MessageCounterCallback callback = MessageCounterCallback.build(CounterType.FIN);
        NSQMessage mockMessage = mockNSQMessage();
        callback.accumulate(cfg, mockMessage);
        callback.accumulate(cfg, mockMessage);
        callback.accumulate(cfg, mockMessage);
        Assert.assertTrue(callback.getLastDeltaDump() > 0l);
        long lastDeltaDelta = callback.getLastDeltaDump();
        Map<Address, Long> deltaMap = callback.dumpDelta();
        Assert.assertEquals(1, deltaMap.size());
        Assert.assertEquals(new Long(3L), deltaMap.get(mockMessage.getAddress()));
        long cnt = callback.getCount();
        Assert.assertEquals(3, cnt);
        Assert.assertTrue(callback.getLastDeltaDump() > lastDeltaDelta);
    }

    private NSQMessage mockNSQMessage() {
        NSQMessage mockMsg = partialMockBuilder(NSQMessage.class)
                .addMockedMethods("getAddress", "getTopicInfo")
                .createMock();
        //String host, String port, String version, String topic, int partition, boolean extend
        expect(mockMsg.getAddress()).andStubReturn(new Address("127.0.0.1", "4151", "ver0.1", "topic", 1, true));
        replayAll();
        return mockMsg;
    }
}
