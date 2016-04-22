package com.youzan.nsq.client.remoting.connector;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;
import com.youzan.nsq.client.commands.Publish;
import com.youzan.nsq.client.enums.ResponseType;
import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.frames.NSQFrame;
import com.youzan.nsq.client.frames.ResponseFrame;
import com.youzan.nsq.client.remoting.ConnectorMonitor;
import com.youzan.nsq.client.remoting.NSQConnector;

/**
 * Created by pepper on 14/12/22.
 */
public class ProducerConnector {
    private static final Logger log = LoggerFactory.getLogger(ProducerConnector.class);
    private String host; // lookupd ip
    private int port; // lookupd port
    private ConcurrentHashMap</* ip:port */String, NSQConnector> connectorMap;
    private AtomicLong index;
    private static final int DEFAULT_RETRY = 3;

    public ProducerConnector(String host, int port) {
        this.host = host;
        this.port = port;
        this.connectorMap = new ConcurrentHashMap<String, NSQConnector>();
        this.index = new AtomicLong(0);
    }

    public ConcurrentHashMap<String, NSQConnector> getConnectorMap() {
        return connectorMap;
    }

    public void connect() {
        List<NSQNode> nodes = ConnectorUtils.lookupNode(host, port);
        if (null == nodes || nodes.isEmpty()) {
            log.error("producer start fail !! could not find any nsqd from lookupd {}:{}", host, port);
            return;
        }

        for (NSQNode nsqNode : nodes) {
            if (ConnectorUtils.isExcluded(nsqNode))
                continue;

            NSQConnector connector = null;
            try {
                connector = new NSQConnector(nsqNode.getHost(), nsqNode.getPort(), null, 0);
                connectorMap.put(ConnectorUtils.getConnectorKey(nsqNode), connector);
            } catch (NSQException e) {
                log.error("Producer: connector to {} goes wrong at:{}", ConnectorUtils.getConnectorKey(nsqNode), e);
                if (connector != null)
                    connector.close();
            }
        }
        
        ConnectorMonitor.getInstance().setLookup(host, port);
        ConnectorMonitor.getInstance().registerProducer(this);
    }

    public boolean put(String topic, String msg) throws NSQException, InterruptedException {
        return put(topic, msg.getBytes());
    }

    public boolean put(String topic, byte[] msgData) throws NSQException, InterruptedException {
        Publish pub = new Publish(topic, msgData);
        NSQConnector connector = getConnector();

        if (connector == null)
            throw new NSQException("No active connector to be used.");

        NSQFrame response = connector.writeAndWait(pub);
        if (response instanceof ResponseFrame) {
            if (((ResponseFrame) response).getResponseType() == ResponseType.OK) {
                return true;
            }
        }
        throw new NSQException(response.getMessage());
    }

    private NSQConnector getConnector() {
        NSQConnector connector = nextConnector();
        if (connector == null) return null;
        int retry = 0;
        while (!connector.isConnected()) {
            if (retry >= DEFAULT_RETRY) {
                connector = null;
                break;
            }
            removeConnector(connector);
            connector = nextConnector();
            retry++;
        }

        return connector;
    }

    private NSQConnector nextConnector() {
        NSQConnector[] connectors = new NSQConnector[connectorMap.size()];
        connectorMap.values().toArray(connectors);
        if (connectors.length < 1) return null;
        Long nextIndex = Math.abs(index.incrementAndGet() % connectors.length);
        return connectors[nextIndex.intValue()];
    }
    
    public boolean removeConnector(NSQConnector connector) {
        if (connector == null) return true;
        log.info("Producer: removeConnector({})", ConnectorUtils.getConnectorKey(connector));
        connector.close();
        return connectorMap.remove(ConnectorUtils.getConnectorKey(connector), connector);
    }
    
    public void addConnector(NSQConnector connector) {
        log.info("Producer: addConnector({})", ConnectorUtils.getConnectorKey(connector));
        connectorMap.put(ConnectorUtils.getConnectorKey(connector), connector);
    }

    public void close() {
        for (NSQConnector connector : connectorMap.values()) {
            connector.close();
        }
    }
}
