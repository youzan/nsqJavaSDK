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
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
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
    private boolean started = false;
    private boolean closing = false;
    private volatile long lastConnecting = 0L;
    private final int _INTERVAL_IN_SECOND = 60;
    private final ScheduledExecutorService scheduler = Executors
            .newSingleThreadScheduledExecutor(new NamedThreadFactory("LookupChecker", Thread.MAX_PRIORITY));

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
            offset = _r.nextInt(100);
            keepLookupServers();
        }
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
        final int delay = _r.nextInt(60); // seconds
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    newLookupServers();
                } catch (FileNotFoundException e) {
                    logger.warn("You run with the lower server version.");
                } catch (Exception e) {
                    logger.error("Exception", e);
                }
            }
        }, delay, _INTERVAL_IN_SECOND, TimeUnit.SECONDS);
    }

    private void newLookupServers() throws IOException {
        if (this.addresses == null || this.addresses.isEmpty()) {
            return;
        }
        if (System.currentTimeMillis() < lastConnecting + TimeUnit.SECONDS.toMillis(_INTERVAL_IN_SECOND)) {
            return;
        }
        lastConnecting = System.currentTimeMillis();
        if (!this.started) {
            if (closing) {
                logger.warn("Having closed the lookup service.");
            }
            return;
        }
        final int index = ((offset++) & Integer.MAX_VALUE) % this.addresses.size();
        final String lookup = this.addresses.get(index);
        final String url = String.format("http://%s/listlookup", lookup);
        logger.debug("Begin to get the new lookup servers. LB: Size: {}, Index: {}, From URL: {}",
                this.addresses.size(), index, url);
        final JsonNode rootNode;
        JsonNode tmpRootNode = null;
        URL lookupUrl = null;
        try {
            lookupUrl = new URL(url);
            tmpRootNode = readFromUrl(lookupUrl);
        } catch (ConnectException ce) {
            //got a connection timeout exception(maybe), what we do here is:
            //1. record the ip&addr of both client and server side for trace debug.
            //2. TODO: improve timeout value of jackson parser to give it a retry, record
            //   a trace about the result, if failed, throws exception to interrupt
            //   lookup checker run().
            _handleConnectionTimeout(lookup, ce);
            return;
        } finally {
            //assign temp root node to rootNode, in both successful case and filed case
            rootNode = tmpRootNode;
        }
        final JsonNode nodes = rootNode.get("lookupdnodes");
        if (null == nodes) {
            logger.error("NSQ Server do response without any lookup servers!");
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
            Collections.sort(newLookups);
            this.addresses = newLookups;
        }
        logger.debug("Recently have got the lookup servers : {}", this.addresses);
    }

    /**
     * request http GET for pass in URL, then parse response to json, some predefined
     * header properties are added here, like Accept: application/vnd.nsq;
     * stream as json
     *
     * @param url
     * @return
     */
    private JsonNode readFromUrl(final URL url) throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(5 * 1000);
        con.setReadTimeout(10 * 1000);
        //skip that, as GET is default operation
        //con.setRequestMethod("GET");
        //add request header, to support nsq of new version
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
        //jackson handles InputStream close operation
        JsonNode treeNode = mapper.readTree(con.getInputStream());
        return treeNode;
    }

    private void _handleConnectionTimeout(String lookup, ConnectException ce) throws IOException {
        String ip = "EMPTY", address = "EMPTY";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ip = addr.getHostAddress().toString();//ip where sdk resides
            address = addr.getHostName().toString();//address where sdk resides
        } catch (Exception e) {
            logger.error("Could not fetch ip or address form local client, should not occur.", e);
        }
        logger.warn("Fail to connect to NSQ lookup. SDK Client, ip:{} address:{}. Remote lookup:{}. Will kick off another try in 60 seconds.", ip, address, lookup);
        logger.warn("Nested connection exception stacktrace:", ce);
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
            final JsonNode rootNode = readFromUrl(new URL(url));
            final JsonNode producers = rootNode.get("producers");
            for (JsonNode node : producers) {
                final String host = node.get("broadcast_address").asText();
                final int port = node.get("tcp_port").asInt();
                final Address address = new Address(host, port);
                dataNodes.add(address);
            }
            logger.debug("The server response info after looking up some DataNodes: {}", rootNode.toString());
            return dataNodes; // maybe it is empty
        } catch (Exception e) {
            final String tip = "SDK can't get the right lookup info. " + url;
            throw new NSQLookupException(tip, e);
        }
    }

    @Override
    public void close() {
        closing = true;
        scheduler.shutdownNow();
    }
}