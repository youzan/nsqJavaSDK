package it.youzan.nsq.client;

import java.util.SortedSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.youzan.nsq.client.core.lookup.LookupServiceImpl;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;

public class ITNSQLookupService {

    private static final Logger logger = LoggerFactory.getLogger(ITNSQLookupService.class);

    @Test
    public void lookup() throws NSQLookupException {
        LookupServiceImpl srv = new LookupServiceImpl("127.0.0.1:4161");
        SortedSet<Address> addresses = srv.lookup("test", true);
        for (final Address addr : addresses) {
            logger.info("Address : {}", addr);
        }
        srv.close();
    }
}
