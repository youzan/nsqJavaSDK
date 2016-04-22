package com.youzan.nsq.client.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;

/**
 * 检索lookupd上所有注册的节点，用于生产者连接
 * 
 * @author maoxiajun
 *
 */
public class Nodes {

    private static final Logger log = LoggerFactory.getLogger(Nodes.class);
    private static final int DEFAULT_TIMEOUT = 5 * 1000;
    private static Set<String> addrs = new HashSet<String>();

    /**
     * 构造器
     * 
     * @param host
     *            lookupd主机
     * @param port
     *            lookupd端口
     */
    public Nodes(String host, int port) {
        String[] hostArr = host.split(",");
        StringBuffer sb = new StringBuffer();
        for (String h : hostArr) {
            sb.append("http://").append(h).append(":").append(port).append("/nodes");
            addrs.add(sb.toString());
            sb.setLength(0);
        }
    }

    /**
     * 检索所有node
     * 
     * @return
     */
    public List<NSQNode> query() {
        List<NSQNode> nodes = new ArrayList<NSQNode>();

        for (String addr : addrs) {
            try {
                nodes = Request.Get(addr).connectTimeout(DEFAULT_TIMEOUT).socketTimeout(DEFAULT_TIMEOUT).execute()
                        .handleResponse(new NodesHandler());

                return nodes;
            } catch (IOException e) {
                log.error("execute nsqlookupd command 'nodes' failed, {}, addr:{}", e.getMessage(), addr);
            }
        }

        return nodes;
    }

}
