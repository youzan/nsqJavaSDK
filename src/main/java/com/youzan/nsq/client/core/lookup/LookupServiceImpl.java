package com.youzan.nsq.client.core.lookup;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class LookupServiceImpl implements LookupService {

    private static final Logger logger = LoggerFactory.getLogger(LookupServiceImpl.class);
    private static final long serialVersionUID = 1773482379917817275L;

    /**
     * JSON Tool
     */
    private static final ObjectMapper mapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    /**
     * the sorted lookup's addresses
     */
    private volatile List<String> addresses;
    /**
     * Load-Balancing Strategy: round-robin
     */
    private volatile int offset = 0;
    private volatile ScheduledExecutorService scheduler;
    private volatile boolean started = false;

    /**
     * @param addresses the lookup addresses
     */
    public LookupServiceImpl(List<String> addresses) {
        initAddresses(addresses);
    }

    /**
     * @param addresses the lookup addresses
     */
    public LookupServiceImpl(String addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        final String[] tmp = addresses.split(",");
        final List<String> tmpList = new ArrayList<>(tmp.length);
        for (String address : tmp) {
            address = address.trim();
            address = address.replace(" ", "");
            tmpList.add(address);
        }
        initAddresses(tmpList);
    }

    private void initAddresses(List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input addresses is blank!");
        }
        Collections.sort(addresses);
        setAddresses(addresses);
    }

    @Override
    public void start() {
        if (!started) {
            // begin: a light implement way
            started = true;
            if (null == scheduler) {
                scheduler = Executors
                        .newSingleThreadScheduledExecutor(new NamedThreadFactory("LookupChecker", Thread.MAX_PRIORITY));
            }
            offset = _r.nextInt(100);
            keepLookupServers();
        }
        started = true;
    }

    /**
     * @param addresses the addresses to set
     */
    private void setAddresses(List<String> addresses) {
        final List<String> tmp = this.addresses;
        this.addresses = addresses;
        if (null != tmp) {
            tmp.clear();
        }
    }

    /**
     * @return the sorted lookup's addresses
     */
    List<String> getAddresses() {
        return addresses;
    }

    /**
     * Asynchronized processing
     */
    private void keepLookupServers() {
        final int delay = _r.nextInt(60) + 45; // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newLookupServers();
                } catch(FileNotFoundException e){
                    logger.warn("You run with the lower server version.");
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        }, delay, 60, TimeUnit.SECONDS);
    }

    private void newLookupServers() throws IOException {
        if (this.addresses == null || this.addresses.isEmpty()) {
            return;
        }
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookup = this.addresses.get(index);
        final String url = String.format("http://%s/listlookup", lookup);
        logger.debug("Begin to get the new lookup servers. LB: Size: {}, Index: {}, From URL: {}",
                this.addresses.size(), index, url);
        final JsonNode rootNode = mapper.readTree(new URL(url));
        final JsonNode nodes = rootNode.get("data").get("lookupdnodes");
        if (null == nodes) {
            logger.error("NSQ Server do response without any lookupd!");
            return;
        }
        final List<String> newLookups = new ArrayList<>(nodes.size());
        for (JsonNode node : nodes) {
            final String host = node.get("NodeIP").asText();
            final int port = node.get("HttpPort").asInt();
            final String address = host + ":" + port;
            newLookups.add(address);
        }
        if (!newLookups.isEmpty()) {
            this.addresses = newLookups;
        }
        logger.debug("Having got the new lookup servers : {}", this.addresses);
    }

    @Override
    public SortedSet<Address> lookup(String topic) throws NSQLookupException {
        return lookup(topic, true);
    }

    @Override
    public SortedSet<Address> lookup(String topic, boolean writable) throws NSQLookupException {
        if (null == topic || topic.isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }
        final SortedSet<Address> dataNodes = new TreeSet<>();
        assert null != this.addresses;
        /*
         * It is unnecessary to use Atomic/Lock for the variable
         */
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookup = this.addresses.get(index);
        final String url = String.format("http://%s/lookup?topic=%s&access=%s", lookup, topic, writable ? "w" : "r"); // readable
        logger.debug("Begin to lookup some DataNodes from URL {}", url);
        try {
            final JsonNode rootNode = mapper.readTree(new URL(url));
            final JsonNode producers = rootNode.get("data").get("producers");
            for (JsonNode node : producers) {
                final String host = node.get("broadcast_address").asText();
                final int port = node.get("tcp_port").asInt();
                final Address address = new Address(host, port);
                dataNodes.add(address);
            }
            logger.debug("The server response info after looking up some DataNodes: {}", rootNode.toString());
            return dataNodes; // maybe it is empty
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