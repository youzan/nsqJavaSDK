package com.youzan.nsq.client.entity;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by lin on 17/10/13.
 */
public class AddressTest {

    @Test
    public void testAddressCompare() {
        //String host, String port, String version, String topic, int partition, boolean extend
        Address addr1 = new Address("127.0.0.1", "4150", "v0.3.8-H.A", "topic", 0, false);
        Address addr2 = new Address("127.0.0.1", "4150", "v0.3.8-H.A", "topic", 0, true);
        Address addr3 = new Address("127.0.0.1", "4150", "v0.3.8-H.A", "topic", 0, true);
        Set except = new HashSet();
        except.add(addr1);
        except.add(addr2);
        except.remove(addr3);
        Assert.assertEquals(1, except.size());

    }
}
