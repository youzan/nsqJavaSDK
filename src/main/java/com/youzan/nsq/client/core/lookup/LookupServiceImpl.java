package com.youzan.nsq.client.core.lookup;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
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
public class LookupServiceImpl implements LookupService {

    private static final Logger logger = LoggerFactory.getLogger(LookupServiceImpl.class);
    private static final long serialVersionUID = 1773482379917817275L;

    /**
     * JSON Tool
     */
    private static final ObjectMapper mapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);;
    /**
     * the sorted lookupd's addresses
     */
    private volatile List<String> addresses;
    /**
     * Load-Balancing Strategy: round-robin
     */
    private volatile int offset;
    private volatile ScheduledExecutorService scheduler;

    public void init() {
        if (null == scheduler) {
            scheduler = Executors
                    .newSingleThreadScheduledExecutor(new NamedThreadFactory("LookupChecker", Thread.MAX_PRIORITY));
        }
        offset = _r.nextInt(100);
        keepLookupServers();
    }

    /**
     * @param addresses
     */
    public LookupServiceImpl(List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        Collections.sort(addresses);
        setAddresses(addresses);
        init();
    }

    /**
     * Give me NSQ-Lookupd addresses
     * 
     * @param addresses
     */
    public LookupServiceImpl(String addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        final String[] tmp = addresses.split(",");
        final List<String> tmpList;
        if (tmp != null) {
            tmpList = new ArrayList<>(tmp.length);
            for (String addr : tmp) {
                addr = addr.trim();
                addr = addr.replace(" ", "");
                tmpList.add(addr);
            }
            Collections.sort(tmpList);
        } else {
            tmpList = new ArrayList<>(0);
        }
        setAddresses(tmpList);
        init();
    }

    /**
     * @param addresses
     *            the addresses to set
     */
    private void setAddresses(List<String> addresses) {
        final List<String> tmp = this.addresses;
        this.addresses = addresses;
        if (null != tmp) {
            tmp.clear();
        }
    }

    /**
     * @return the sorted lookupd's addresses
     */
    public List<String> getAddresses() {
        return addresses;
    }

    /**
     * Asynchronized processing
     */
    public void keepLookupServers() {
        final int delay = _r.nextInt(60) + 45; // seconds
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                newLookupServers();
            } catch (Exception e) {
                logger.error("Exception", e);
            }
        }, delay, 60, TimeUnit.SECONDS);
    }

    /**
     * @throws IOException
     * @throws JsonProcessingException
     * @throws MalformedURLException
     */
    private void newLookupServers() throws IOException, JsonProcessingException, MalformedURLException {
        if (this.addresses == null || this.addresses.isEmpty()) {
            return;
        }
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookupd = this.addresses.get(index);
        final String url = String.format("http://%s/listlookup", lookupd);
        logger.debug("Size: {}, Index: {}, URL: {}", this.addresses.size(), index, url);
        final JsonNode rootNode = mapper.readTree(new URL(url));
        JsonNode t = rootNode.get("data");
        final JsonNode nodes = rootNode.get("data").get("lookupdnodes");
        if (null == nodes) {
            logger.error("NSQ Server do response without any lookupd!");
            return;
        }
        final List<String> newLookupds = new ArrayList<>(nodes.size());
        for (JsonNode node : nodes) {
            final int id = node.get("ID").asInt();
            final String host = node.get("NodeIP").asText();
            final int port = node.get("HttpPort").asInt();
            String addr = host + ":" + port;
            newLookupds.add(addr);
        }
        if (!newLookupds.isEmpty()) {
            this.addresses = newLookupds;
        }
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
        assert null != this.addresses;
        /**
         * It is unnecessary to use Atomic/Lock for the variable
         */
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookupd = this.addresses.get(index);
        final String url = String.format("http://%s/lookup?topic=%s&access=%s", lookupd, topic, writable ? "w" : "r"); // readable
        try {
            logger.debug("Begin to lookup for getting NSQd...");
            final JsonNode rootNode = mapper.readTree(new URL(url));
            final JsonNode producers = rootNode.get("data").get("producers");
            for (JsonNode node : producers) {
                final String host = node.get("broadcast_address").asText();
                final int port = node.get("tcp_port").asInt();
                final Address addr = new Address(host, port);
                nsqds.add(addr);
            }
            logger.debug("Server response info : {}", rootNode.toString());
            return nsqds; // maybe it is empty
        } catch (Exception e) {
            final String tip = "SDK can't get the right lookup info.";
            throw new NSQLookupException(tip, e);
        }
    }

    @Override
    public void close() {
        if (null != scheduler) {
            scheduler.shutdownNow();
        }
    }

}
