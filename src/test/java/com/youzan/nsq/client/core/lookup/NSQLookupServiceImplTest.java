package com.youzan.nsq.client.core.lookup;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.log4testng.Logger;

import com.youzan.util.IOUtil;

public class NSQLookupServiceImplTest {
    private static final Logger logger = Logger.getLogger(NSQLookupServiceImplTest.class);

    @Test
    public void NSQLookupServiceImpl() {
        NSQLookupServiceImpl srv = null;
        try {
            srv = new NSQLookupServiceImpl("10.232.120.12:6411");
            for (String addr : srv.getAddresses()) {
                Assert.assertTrue(addr.split(":").length == 2);
            }
        } finally {
            IOUtil.closeQuietly(srv);
        }
    }

    @Test
    public void lookup() {
    }
}
