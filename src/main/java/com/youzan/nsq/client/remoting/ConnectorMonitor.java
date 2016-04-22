package com.youzan.nsq.client.remoting;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;
import com.youzan.nsq.client.exceptions.NSQException;
import com.youzan.nsq.client.remoting.connector.ConnectorUtils;
import com.youzan.nsq.client.remoting.connector.CustomerConnector;
import com.youzan.nsq.client.remoting.connector.ProducerConnector;

/**
 * 连接状态监视器，发现连接断开时启动重连
 * @author maoxiajun
 *
 */
public class ConnectorMonitor {
	private static final Logger log = LoggerFactory.getLogger(ConnectorMonitor.class);
	private static final int DEFAULT_RECONNECT_PERIOD = 60 * 1000;
	private HashSet<ProducerConnector> producers = new HashSet<ProducerConnector>();
    private HashSet<CustomerConnector> consumers = new HashSet<CustomerConnector>();
    private String lookupHost;
    private int lookupPort;
    
    private ConnectorMonitor(){
        monitor();
    }
    
    private static class ConnectorMonitorHolder {
        static final ConnectorMonitor instance = new ConnectorMonitor();
    }
    
    public static ConnectorMonitor getInstance(){
        return ConnectorMonitorHolder.instance;
    }
	
	private void monitor() {
	    Thread monitor = new Thread(new Runnable() {
            
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(DEFAULT_RECONNECT_PERIOD);
                    } catch (InterruptedException e) {
                        log.warn("NSQ connector monitor sleep goes wrong at:{}", e);
                    }
                    try {
                        dealProducer();
                        dealCustomer();
                    } catch (Exception e) {
                        log.error("Monitor deal goes wrong at:{}", e);
                    }
                }
            }
        });
	    monitor.setName("ConnectorMonitorThread");
	    monitor.setDaemon(true);
	    monitor.start();
	}
	
	private void dealProducer() {
	    List<NSQNode> nodes = ConnectorUtils.lookupNode(lookupHost, lookupPort);
	    
	    for (ProducerConnector producer : producers) {
	        ConcurrentHashMap<String, NSQConnector> connectorMap = producer.getConnectorMap();
	        List<NSQNode> oldNodes = new ArrayList<NSQNode>();
	        for (NSQConnector connector : connectorMap.values()) {
	            if (!connector.isConnected())
	                producer.removeConnector(connector);
	            else 
	                oldNodes.add(new NSQNode(connector.getHost(), connector.getPort()));
	        }
	        
	        for (NSQNode node : nodes) {
	            if (!oldNodes.contains(node) && !ConnectorUtils.isExcluded(node)) {
	                NSQConnector connector = null;
                    try {
                        connector = new NSQConnector(node.getHost(), node.getPort(), null, 0);
                        producer.addConnector(connector);
                    } catch (NSQException e) {
                        log.warn("Producer monitor: connector to ({}:{}) failed.", node.getHost(), node.getPort());
                        if (connector != null)
                            connector.close();
                    }
	            }
	        }
	    }
	}
	
	private void dealCustomer() {
	    for (CustomerConnector customer : consumers) {
	        List<NSQNode> nodes = ConnectorUtils.lookupTopic(lookupHost, lookupPort, customer.getTopic());
	        ConcurrentHashMap<String, NSQConnector> connectorMap = customer.getConnectorMap();
	        List<NSQNode> oldNodes = new ArrayList<NSQNode>();
	        
	        for (NSQConnector connector : connectorMap.values()) {
	            if (!connector.isConnected())
                    customer.removeConnector(connector);
                else 
                    oldNodes.add(new NSQNode(connector.getHost(), connector.getPort()));
	        }
	        
	        for (NSQNode node : nodes) {
                if (!oldNodes.contains(node)) {
                    NSQConnector connector = null;
                    try {
                        connector = new NSQConnector(node.getHost(), node.getPort(), customer.getSubListener(), customer.getReadyCount());
                        connector.sub(customer.getTopic(), customer.getChannel());
                        connector.rdy(customer.getReadyCount());
                        connectorMap.put(ConnectorUtils.getConnectorKey(node), connector);
                    } catch (NSQException e) {
                        log.error("Customer: connector to {} goes wrong at:{}", ConnectorUtils.getConnectorKey(node), e);
                        if (connector != null)
                            connector.close();
                    }
                }
            }
	    }
	}
	
	public void registerProducer(ProducerConnector producer) {
		if (null == producer) {
			return;
		}
		producers.add(producer);
	}

	public void registerConsumer(CustomerConnector connector) {
		if (null == connector) {
			return;
		}
		consumers.add(connector);
	}
	
	public void setLookup(String lookupHost, int lookupPort) {
	    this.lookupHost = lookupHost;
	    this.lookupPort = lookupPort;
	}
}