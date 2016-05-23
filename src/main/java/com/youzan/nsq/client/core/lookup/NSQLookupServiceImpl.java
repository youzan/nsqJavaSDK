package com.youzan.nsq.client.core.lookup;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.util.NamedThreadFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class NSQLookupServiceImpl implements NSQLookupService {

    private static final Logger logger = LoggerFactory.getLogger(NSQLookupServiceImpl.class);
    private static final long serialVersionUID = 1773482379917817275L;

    /**
     * JSON Tool
     */
    private static final ObjectMapper mapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);;

    /**
     * the sorted lookupd's addresses
     */
    private final List<String> addresses;
    /**
     * Load-Balancing Strategy: round-robin
     */
    private volatile int offset;
    private ScheduledExecutorService scheduler;

    public void init() {
        final Random r = new Random(10000);
        offset = r.nextInt(100);

        if (null == scheduler) {
            scheduler = Executors
                    .newSingleThreadScheduledExecutor(new NamedThreadFactory("LookupChecker", Thread.MAX_PRIORITY));
        }
        this.checkLookupServers();
    }

    /**
     * @param addresses
     */
    public NSQLookupServiceImpl(List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        this.addresses = addresses;
        Collections.sort(this.addresses);
        init();
    }

    /**
     * Give me NSQ-Lookupd addresses
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(String addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        final String[] tmp = addresses.split(",");
        if (tmp != null) {
            this.addresses = new ArrayList<>(tmp.length);
            for (String addr : tmp) {
                addr = addr.trim();
                addr = addr.replace(" ", "");
                this.addresses.add(addr);
            }
            Collections.sort(this.addresses);
        } else {
            this.addresses = new ArrayList<>(0);
        }
        init();
    }

    /**
     * @return the sorted lookupd's addresses
     */
    public List<String> getAddresses() {
        return addresses;
    }

    /**
     * Asynchronized
     */
    private void checkLookupServers() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {

            } catch (Exception e) {
                logger.error("Lookup be wrong!", e);
            }
        }, 1 * 60, 1 * 60, TimeUnit.SECONDS);
    }

    @Override
    public SortedSet<Address> lookup(String topic) throws NSQLookupException {
        return lookup(topic, true);
    }

    /**
     * @param topic
     * @param writable
     * @return the NSQd addresses. The count must be not too large.
     */
    @Override
    public SortedSet<Address> lookup(String topic, boolean writable) throws NSQLookupException {
        if (null == topic || topic.isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }
        final SortedSet<Address> nsqds = new TreeSet<>();
        /**
         * It is unnecessary to use Atomic/Lock for the variable
         */
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookupd = this.addresses.get(index);
        try {
            final String url = String.format("http://%s/lookup?topic=%s&access=%s", lookupd, topic,
                    writable ? "w" : "r"); // readable

            // final String url = String.format("http://%s/lookup?topic=%s",
            // lookupd, topic);
            JsonNode rootNode = mapper.readTree(new URL(url));
            JsonNode producers = rootNode.get("data").get("producers");
            for (JsonNode node : producers) {
                final String host = node.get("broadcast_address").asText();
                final int port = node.get("tcp_port").asInt();
                final Address addr = new Address(host, port);
                nsqds.add(addr);
            }
            logger.debug("Server response info : {}", rootNode.toString());
        } catch (Exception e) {
            final String tip = "SDK can't get the right lookup info.";
            logger.error(tip, e);
            throw new NSQLookupException(tip, e);
        }
        return nsqds;
    }

    @Override
    public void close() {
        this.scheduler.shutdownNow();
    }

}
