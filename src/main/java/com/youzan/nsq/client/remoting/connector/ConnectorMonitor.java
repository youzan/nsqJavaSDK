package com.youzan.nsq.client.remoting.connector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;
import com.youzan.nsq.client.remoting.NSQConnector;
import com.youzan.util.IOUtil;

/**
 * 连接状态监视器，发现连接断开时启动重连
 * 
 * @author maoxiajun
 *
 */
public class ConnectorMonitor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConnectorMonitor.class);

    private HashSet<ProducerConnector> producers = new HashSet<ProducerConnector>();
    private HashSet<CustomerConnector> consumers = new HashSet<CustomerConnector>();
    private String host;
    private int port;

    /**
     * @param host
     * @param port
     */
    public ConnectorMonitor(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private void dealProducer() {
        // 服务端的最新节点
        List<NSQNode> nodes = ConnectorUtils.lookupNode(host, port);

        for (ProducerConnector producer : producers) {
            ConcurrentHashMap<String, NSQConnector> connectorMap = producer.getConnectorMap();
            // 当前内存保存的节点, 变成过时的节点
            List<NSQNode> oldNodes = new ArrayList<NSQNode>();
            for (NSQConnector connector : connectorMap.values()) {
                if (!connector.isConnected()) {
                    producer.removeConnector(connector);
                } else {
                    oldNodes.add(new NSQNode(connector.getHost(), connector.getPort()));
                }
            }

            for (NSQNode node : nodes) {
                if (!oldNodes.contains(node) && !ConnectorUtils.isExcluded(node)) {
                    NSQConnector connector = null;
                    try {
                        connector = new NSQConnector(node.getHost(), node.getPort(), null, 0);
                        producer.addConnector(connector);
                    } catch (Exception e) {
                        log.error("Producer monitor: connector to ({}:{}) failed.", node.getHost(), node.getPort());
                        log.error("", e);
                        if (connector != null) {
                            connector.close();
                        }
                    }
                }
            }
        }
    }

    private void dealCustomer() {
        for (CustomerConnector customer : consumers) {
            List<NSQNode> nodes = ConnectorUtils.lookupTopic(host, port, customer.getTopic());
            ConcurrentHashMap<String, NSQConnector> connectorMap = customer.getConnectorMap();
            List<NSQNode> oldNodes = new ArrayList<NSQNode>();

            for (NSQConnector connector : connectorMap.values()) {
                if (!connector.isConnected()) {
                    customer.removeConnector(connector);
                    IOUtil.closeQuietly(connector);
                } else {
                    oldNodes.add(new NSQNode(connector.getHost(), connector.getPort()));
                }
            }

            for (NSQNode node : nodes) {
                if (!oldNodes.contains(node)) {
                    NSQConnector connector = null;
                    try {
                        connector = new NSQConnector(node.getHost(), node.getPort(), customer.getSubListener(),
                                customer.getReadyCount());
                        connector.sub(customer.getTopic(), customer.getChannel());
                        connector.rdy(customer.getReadyCount());
                        connectorMap.put(ConnectorUtils.getConnectorKey(node), connector);
                    } catch (Exception e) {
                        final String tips = "CustomerConnector can not connect " + ConnectorUtils.getConnectorKey(node);
                        log.error(tips, e);
                        IOUtil.closeQuietly(connector);
                    }
                }
            }
        }
    }

    void registerProducer(ProducerConnector producer) {
        if (null == producer) {
            return;
        }
        producers.add(producer);
    }

    void registerConsumer(CustomerConnector connector) {
        if (null == connector) {
            return;
        }
        consumers.add(connector);
    }

    @Override
    public void run() {
        try {
            dealCustomer();
        } catch (Exception e) {
            log.error("Monitoring of Consumers error", e);
        }

        try {
            dealProducer();
        } catch (Exception e) {
            log.error("Monitoring of Producers error", e);
        }
    }

}
