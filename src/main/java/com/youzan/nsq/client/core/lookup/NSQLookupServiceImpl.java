package com.youzan.nsq.client.core.lookup;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.exception.NSQLookupException;

public class NSQLookupServiceImpl implements NSQLookupService {

    private static final long serialVersionUID = 1773482379917817275L;

    private static final Logger logger = LoggerFactory.getLogger(NSQLookupServiceImpl.class);

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
    private int offset;

    public void init() {
        final Random r = new Random(10000);
        offset = r.nextInt(100);
    }

    /**
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(List<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("Your input 'addresses' is blank!");
        }
        this.addresses = addresses;
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
        String[] tmp = addresses.split(",");
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

    @Override
    public SortedSet<Address> lookup(String topic) throws NSQLookupException {
        return lookup(topic, true);
    }

    /**
     * 
     * @param topic
     * @param writable
     * @return the NSQd addresses . The count must be not too large.
     */
    @Override
    public SortedSet<Address> lookup(String topic, boolean writable) throws NSQLookupException {
        final SortedSet<Address> nsqds = new TreeSet<>();
        /**
         * It is unnecessary to use Atomic/Lock for the variable
         */
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookupd = this.addresses.get(index);
        try {
            // @since JDK8 String
            // final String url =
            // String.format("http://%s/lookup?topic=%s&access=%s", lookupd,
            // topic,
            // writable ? "w" : "r");

            final String url = String.format("http://%s/lookup?topic=%s", lookupd, topic);
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
    public boolean save() {
        // TODO - implement NSQLookupServiceImpl.save
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean load() {
        // TODO - implement NSQLookupServiceImpl.load
        throw new UnsupportedOperationException();
    }

    /**
     * @return the addresses
     */
    public List<String> getAddresses() {
        return addresses;
    }
}
