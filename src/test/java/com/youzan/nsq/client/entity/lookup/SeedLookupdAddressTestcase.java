package com.youzan.nsq.client.entity.lookup;

import com.google.common.collect.Sets;
import com.youzan.nsq.client.entity.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by lin on 16/12/14.
 */
public class SeedLookupdAddressTestcase {
    private final static Logger logger = LoggerFactory.getLogger(SeedLookupdAddressTestcase.class);

    public static final String[] seedlookupds = new String[]{"sqs-qa.s.qima-inc.com:4161"};
    public static final String[] seedlookupdsAdmin = new String[]{"http://sqs-qa.s.qima-inc.com:4171"};
    public static final String[] dailySeedlookupds = new String[]{"nsq-daily.s.qima-inc.com:4161"};
    public static final String[] dailySeedlookupdsAdmin = new String[]{"http://nsq-daily.s.qima-inc.com:4171"};
    public static final String[] dummyLookupds = new String[]{"dummyLookupd1:4161", "dummyLookupd2:4161", "dummyLookupd3:4161"};
    public static final String[] lookupdAddresses = new String[]{"10.9.80.209:4260:4162", "10.9.80.209:4262", "10.9.52.215:4161"};

    @Test
    public void testLookupAddressesUpdate() throws NoSuchFieldException, IllegalAccessException {
        SeedLookupdAddress aSeed = null, aDummySeed = null, aDuplicatedSeed = null;
        try {
            logger.info("[testLookupAddressesUpdate] starts.");
            aSeed = SeedLookupdAddress.create(seedlookupds[0]);

            int num = SeedLookupdAddress.listAllLookup();
            Assert.assertEquals(num, 1);

            List<LookupdAddress> lookupdAddresses = dumpLookupdAddresses(aSeed);
            Set<String> lookupAddrs = new HashSet<>();
            for(LookupdAddress lookupAddr:lookupdAddresses) {
                lookupAddrs.add(lookupAddr.getAddress());
            }

            aSeed.setAddress(dailySeedlookupds[0]);
            aSeed.setClusterId(dailySeedlookupds[0]);

            SeedLookupdAddress.listAllLookup();

            List<LookupdAddress> lookupdAddressesSec = dumpLookupdAddresses(aSeed);
            Set<String> lookupAddrsSec = new HashSet<>();
            for(LookupdAddress lookupAddr:lookupdAddressesSec) {
                lookupAddrsSec.add(lookupAddr.getAddress());
            }
            Assert.assertEquals(Sets.difference(lookupAddrs, lookupAddrsSec).size(), lookupAddrs.size());
        }finally {
            logger.info("[testLookupAddressesUpdate] ends.");
            clean(aSeed);
        }
    }

    @Test
    public void testSeedLookupdAddressCreate() throws NoSuchFieldException, IllegalAccessException {
        SeedLookupdAddress aSeed = null, aDummySeed = null, aDuplicatedSeed = null;
        try {
            logger.info("[testSeedLookupdAddressCreate] starts.");
            aSeed = SeedLookupdAddress.create(seedlookupds[0]);
            aDummySeed = SeedLookupdAddress.create(dummyLookupds[0]);
            aDuplicatedSeed = SeedLookupdAddress.create(seedlookupds[0]);

            Assert.assertTrue(aSeed == aDuplicatedSeed);
            ConcurrentHashMap<String, SeedLookupdAddress> seedMap = dumpSeedLookupdMap(aSeed);
            Assert.assertEquals(seedMap.size(), 2);
            Assert.assertNotNull(seedMap.containsKey(aSeed.getAddress()));
            Assert.assertNotNull(seedMap.containsKey(aDummySeed.getAddress()));
        }finally {
            logger.info("[testSeedLookupdAddressCreate] ends.");
            clean(aSeed);
            clean(aDummySeed);
            clean(aDuplicatedSeed);
        }
    }

    @Test
    public void testLookupdAddressReferenceAdd() throws InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(30);
        try{
            logger.info("[testLookupdAddressReferenceAdd] starts.");
            final LookupdAddress aLookupd = LookupdAddress.create(seedlookupds[0], lookupdAddresses[0]);

            final CountDownLatch latchAdd = new CountDownLatch(20);
            long start = System.currentTimeMillis();
            for(int i = 0; i < 20; i++)
                exec.submit(new Runnable(){
                    @Override
                    public void run() {
                        for(int i = 0; i < 5; i++)
                            aLookupd.addReference();
                        latchAdd.countDown();
                    }
                });
            logger.info("Takes {} millisecs to finish adding reference to LookupdAddress.", System.currentTimeMillis() - start);
            Assert.assertTrue(latchAdd.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(100L, aLookupd.getReferenceCount());

            //start removing reference
            final CountDownLatch latchMinus = new CountDownLatch(20);
            long startMinus = System.currentTimeMillis();
            for(int i = 0; i < 20; i++)
                exec.submit(new Runnable(){
                    @Override
                    public void run() {
                        for(int i = 0; i < 5; i++)
                            aLookupd.removeReference();
                        latchMinus.countDown();
                    }
                });
            logger.info("Takes {} millisecs to finish minusing reference to LookupdAddress.", System.currentTimeMillis() - startMinus);
            Assert.assertTrue(latchMinus.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(100L, aLookupd.getReferenceCount());

        }finally {
            logger.info("[testLookupdAddressReferenceAdd] ends.");
        }
    }

    @Test
    public void testSeedLookupdAddressReferenceAdd() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        ExecutorService exec = Executors.newFixedThreadPool(30);
        final SeedLookupdAddress aSeed = SeedLookupdAddress.create(seedlookupds[0]);
        try {
            logger.info("[testSeedLookupdAddressReferenceAdd] starts.");
            final CountDownLatch latchAdd = new CountDownLatch(20);
            long startAdd = System.currentTimeMillis();
            for(int i = 0; i < 20; i++)
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < 5; i++)
                            SeedLookupdAddress.addReference(aSeed);
                        latchAdd.countDown();
                    }
                });
            Assert.assertTrue(latchAdd.await(1, TimeUnit.SECONDS));
            logger.info("Takes {} millisecs to finish adding references.", System.currentTimeMillis() - startAdd);
            Assert.assertEquals(aSeed.getReferenceCount(), 100);

            final CountDownLatch latchMinus = new CountDownLatch(25);
            long startMinus = System.currentTimeMillis();
            for(int i = 0; i < 25; i++) {
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < 4; i++)
                            SeedLookupdAddress.removeReference(aSeed);
                        latchMinus.countDown();
                    }
                });
            }
            Assert.assertTrue(latchMinus.await(1, TimeUnit.SECONDS));
            logger.info("Takes {} millisecs to finish minusing references.", System.currentTimeMillis() - startMinus);
            Assert.assertEquals(aSeed.getReferenceCount(), 0);

            //last minus, will remove aSeed itself
            SeedLookupdAddress.removeReference(aSeed);
            ConcurrentHashMap<String, SeedLookupdAddress> seedsMap = dumpSeedLookupdMap(aSeed);
            Assert.assertEquals(seedsMap.size(), 0);

        }finally {
            logger.info("[testSeedLookupdAddressReferenceAdd] ends.");
            exec.shutdownNow();
            clean(aSeed);
        }
    }

    @Test
    public void testLookupAddressOps() throws NoSuchFieldException, IllegalAccessException {
        //add lookup address and remove
        SeedLookupdAddress aSeed = null;
        try {
            logger.info("[testLookupAddressOps] starts.");
            Topic topic = new Topic("testLookupAddressOps");
            aSeed = SeedLookupdAddress.create(seedlookupds[0]);
            LookupdAddress aLookup1 = LookupdAddress.create(aSeed.getClusterId(), lookupdAddresses[0]);
            LookupdAddress aLookup2 = LookupdAddress.create(null, lookupdAddresses[1]);
            Assert.assertNull(aLookup2);
            ConcurrentHashMap<String, LookupdAddress> lookupdMap = dumpLookupdMap(aLookup1);
            Assert.assertEquals(1, lookupdMap.size());
            //add and punch one lookup address
            aSeed.addLookupdAddress(aLookup1);
            String lookupStr = aSeed.punchLookupdAddressStr(false);
            Assert.assertEquals(lookupdAddresses[0], lookupStr);
        }finally {
            logger.info("[testLookupAddressOps] ends.");
            clean(aSeed);
        }
    }

    @Test
    public void testListLookup() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, NoSuchFieldException {
        SeedLookupdAddress aSeed = null, anotherSeed = null;
        try{
            logger.info("[testListLookup] starts");
            aSeed = SeedLookupdAddress.create(seedlookupds[0]);
            listLookup(aSeed);
            List<LookupdAddress> lookupAddressRefs = dumpLookupdAddresses(aSeed);
            Assert.assertTrue(lookupAddressRefs.size() > 0);
            for (LookupdAddress lookupdRef : lookupAddressRefs) {
                LookupdAddress lookupdAddress = lookupdRef;
                Assert.assertEquals(1, lookupdAddress.getReferenceCount());
            }

            //test lookup address cache
            anotherSeed = SeedLookupdAddress.create(seedlookupds[0]);
            listLookup(anotherSeed);
        }finally {
            logger.info("[testListLookup] ends.");
            clean(aSeed);
            clean(anotherSeed);
        }
    }

    private void listLookup(final SeedLookupdAddress aSeed) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        Class claz = SeedLookupdAddress.class;
        Method listLookupMethod = claz.getDeclaredMethod("listLookup");
        listLookupMethod.setAccessible(true);
        listLookupMethod.invoke(aSeed);
    }

    @Test
    public void testPunchLookupdAddress() throws NoSuchFieldException, IllegalAccessException {
        SeedLookupdAddress aSeed = null;
        try{
            logger.info("[testPunchLookupdAddress] starts.");
            aSeed = SeedLookupdAddress.create(seedlookupds[0]);
            LookupdAddress aLookupd1 = LookupdAddress.create(aSeed.getClusterId(), lookupdAddresses[0]);
            LookupdAddress aLookupd2 = LookupdAddress.create(aSeed.getClusterId(), lookupdAddresses[1]);
            //duplicated lookup address
            LookupdAddress aLookupd3 = LookupdAddress.create(aSeed.getClusterId(), lookupdAddresses[1]);
            aSeed.addLookupdAddress(aLookupd1);
            aSeed.addLookupdAddress(aLookupd2);
            aSeed.addLookupdAddress(aLookupd3);
            List<LookupdAddress> lookupAddresses = dumpLookupdAddresses(aSeed);
            Assert.assertEquals(2, lookupAddresses.size());

        }finally {
            logger.info("[testPunchLookupdAddress] ends.");
            clean(aSeed);
        }
    }

    @Test
    public void testInvalidLookupInReturnedListLookup(){
        SeedLookupdAddress aSeed = null;
        try{
            logger.info("[testInvalidLookupInReturnedListLookup] starts.");
            //in this situation, a invalid lookupd added and seedlookup address is normal now.
            aSeed = SeedLookupdAddress.create(SeedLookupdAddressTestcase.seedlookupds[0]);
            LookupdAddress aLookupd1 = LookupdAddress.create(aSeed.getClusterId(), SeedLookupdAddressTestcase.lookupdAddresses[0]);
            LookupdAddress aLookupd2 = LookupdAddress.create(aSeed.getClusterId(), SeedLookupdAddressTestcase.lookupdAddresses[1]);
            //duplicated lookup address
            LookupdAddress aLookupd3 = LookupdAddress.create(aSeed.getClusterId(), SeedLookupdAddressTestcase.dummyLookupds[0]);
            aSeed.addLookupdAddress(aLookupd1);
            aSeed.addLookupdAddress(aLookupd2);
            aSeed.addLookupdAddress(aLookupd3);

            String lookupStr1 = aSeed.punchLookupdAddressStr(false);
            Assert.assertEquals(aLookupd1.getAddress(), lookupStr1);

            String lookupStr2 = aSeed.punchLookupdAddressStr(false);
            Assert.assertEquals(aLookupd2.getAddress(), lookupStr2);

            String lookupStr3 = aSeed.punchLookupdAddressStr(false);
            Assert.assertEquals(aLookupd3.getAddress(), lookupStr3);
        }finally {
            logger.info("[testInvalidLookupInReturnedListLookup] ends.");
        }
    }

    private static List<LookupdAddress> dumpLookupdAddresses(final SeedLookupdAddress aSeed) throws NoSuchFieldException, IllegalAccessException {
        Class claz = SeedLookupdAddress.class;
        Field lookupdAddressRefsField = claz.getDeclaredField("lookupAddressesRefs");
        lookupdAddressRefsField.setAccessible(true);
        List<LookupdAddress> lookupAddressRefs = (List<LookupdAddress>) lookupdAddressRefsField.get(aSeed);
        return lookupAddressRefs;
    }

    public static ConcurrentHashMap<String, SeedLookupdAddress> dumpSeedLookupdMap(SeedLookupdAddress seed) throws NoSuchFieldException, IllegalAccessException {
        Class claz = SeedLookupdAddress.class;
        Field seedLookupMapField = claz.getDeclaredField("seedLookupMap");
        seedLookupMapField.setAccessible(true);
        ConcurrentHashMap<String, SeedLookupdAddress> seedMap = (ConcurrentHashMap<String, SeedLookupdAddress>) seedLookupMapField.get(seed);
        return seedMap;
    }

    private static ConcurrentHashMap<String, LookupdAddress> dumpLookupdMap(LookupdAddress lookupd) throws NoSuchFieldException, IllegalAccessException {
        Class claz = LookupdAddress.class;
        Field lookupMapField = claz.getDeclaredField("lookupMap");
        lookupMapField.setAccessible(true);
        ConcurrentHashMap<String, LookupdAddress> lookupdMap = (ConcurrentHashMap<String, LookupdAddress>) lookupMapField.get(lookupd);
        return lookupdMap;
    }

    private static void cleanSeedLookupdAddressMap(SeedLookupdAddress aSeed) throws NoSuchFieldException, IllegalAccessException {
        ConcurrentHashMap<String, SeedLookupdAddress> seedMap = dumpSeedLookupdMap(aSeed);
        seedMap.clear();
    }

    public void clean(SeedLookupdAddress aSeed) throws NoSuchFieldException, IllegalAccessException {
        aSeed.clean();
        cleanSeedLookupdAddressMap(aSeed);
    }
}
