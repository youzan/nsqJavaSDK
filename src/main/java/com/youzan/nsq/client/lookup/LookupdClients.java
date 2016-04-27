package com.youzan.nsq.client.lookup;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;

/**
 * 通过http协议连接nsqlookupd
 * 
 * @author maoxiajun
 *
 */
public class LookupdClients {
    private static final Logger log = LoggerFactory.getLogger(LookupdClients.class);

    /**
     * lookup command
     * 
     * @param host
     * @param port
     * @param topic
     * @return
     */
    public static List<NSQNode> lookup(String host, int port, String topic) {
        if (null == host || null == topic || port < 0) {
            log.error("invalid input of host/topic");
            return Collections.emptyList();
        }

        return new Lookup(host, port).query(topic);
    }

    /**
     * nodes command
     * 
     * @param host
     * @param port
     * @return
     */
    public static List<NSQNode> nodes(String host, int port) {
        if (null == host || port < 0) {
            log.error("invalid input of host/port");
            return Collections.emptyList();
        }

        return new Nodes(host, port).query();
    }
}
