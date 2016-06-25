package com.youzan.nsq.client.core.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;

public class TestNSQLookupService {
    private static final Logger logger = LoggerFactory.getLogger(TestNSQLookupService.class);

    @DataProvider
    public Object[][] genIPs() {
        Object[][] objs = new Object[2][2];

        String ips = "10.232.120.12:6411";
        List<String> expected = new ArrayList<>(10);
        expected.add("10.232.120.12:6411");
        objs[0][0] = ips;
        objs[0][1] = expected;

        ips = "10.232.120.13 : 6411";
        expected = new ArrayList<>(10);
        expected.add("10.232.120.13:6411");
        objs[1][0] = ips;
        objs[1][1] = expected;

        return objs;
    }

    @Test
    public void simpleInit() throws IOException {
        LookupServiceImpl srv = null;
        try {
            srv = new LookupServiceImpl("10.232.120.12:6411");
            for (String addr : srv.getAddresses()) {
                Assert.assertTrue(addr.split(":").length == 2);
                Assert.assertEquals(addr, "10.232.120.12:6411");
            }
        } finally {
        }
    }

    @Test(dataProvider = "genIPs")
    public void testInit(String ips, List<String> expected) {
        LookupServiceImpl srv = null;
        try {
            srv = new LookupServiceImpl(ips);
            Assert.assertEquals(srv.getAddresses(), expected);
        } finally {
        }
    }

    @Test
    public void lookup() throws NSQLookupException {
        LookupServiceImpl srv = new LookupServiceImpl("127.0.0.1:4161");
        SortedSet<Address> addresses = srv.lookup("test", true);
        for (final Address addr : addresses) {
            logger.info("Address : {}", addr);
        }
    }
}
