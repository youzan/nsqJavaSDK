package com.youzan.nsq.client.remoting.connector;

import java.util.List;

import com.youzan.nsq.client.bean.NSQNode;
import com.youzan.nsq.client.lookup.LookupdClients;
import com.youzan.nsq.client.remoting.NSQConnector;

/**
 * @author caohaihong
 * since 2015年10月29日 下午5:48:03
 */
public class ConnectorUtils {
    private static final String[] excludedNodes = { "10.10.11.195", "10.10.39.161", "10.10.47.220", "10.10.74.156",
        "10.10.78.67", "bc-daemon4", "bc-daemon5" };
    
    public static boolean isExcluded(NSQNode node) {
        for (String ip : excludedNodes) {
            if (ip.trim().equalsIgnoreCase(node.getHost().trim())) {
                return true;
            }
        }
        return false;
    }

    public static String getConnectorKey(NSQNode node) {
        StringBuffer sb = new StringBuffer();
        sb.append(node.getHost()).append(":").append(node.getPort());
        return sb.toString();
    }

    public static String getConnectorKey(NSQConnector connector) {
        StringBuffer sb = new StringBuffer();
        sb.append(connector.getHost()).append(":").append(connector.getPort());
        return sb.toString();
    }
    
    public static List<NSQNode> lookupTopic(String lookupHost, int lookupPort, String topic) {
        return LookupdClients.lookup(lookupHost, lookupPort, topic);
    }
    
    public static List<NSQNode> lookupNode(String lookupHost, int lookupPort) {
        return LookupdClients.nodes(lookupHost, lookupPort);
    }
}
