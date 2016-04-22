package com.youzan.nsq.client.lookup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youzan.nsq.client.bean.NSQNode;

/**
 * lookup 命令，用于查找topic所有的可能实例
 * 
 * @author maoxiajun
 *
 */
public class Lookup {

    private static final Logger log = LoggerFactory.getLogger(Lookup.class);
    private static final int DEFAULT_TIMEOUT = 5 * 1000;
    private static Set<String> addrs = new HashSet<String>();

    public Lookup(String host, int port) {
        String[] hostArr = host.split(",");
        StringBuffer sb = new StringBuffer();
        for (String h : hostArr) {
            sb.append("http://").append(h).append(":").append(port).append("/lookup?topic=");
            addrs.add(sb.toString());
            sb.setLength(0);
        }
    }

    /**
     * 检索节点数据
     * 
     * @param topic
     * @return
     */
    public List<NSQNode> query(String topic) {
        List<NSQNode> producers = new ArrayList<NSQNode>();

        StringBuffer sb = new StringBuffer();
        for (String addr : addrs) {
            sb.append(addr).append(topic);
            try {
                producers = Request.Get(sb.toString()).connectTimeout(DEFAULT_TIMEOUT).socketTimeout(DEFAULT_TIMEOUT)
                        .execute().handleResponse(new LookupHandler());

                return producers;
            } catch (Exception e) {
                log.error("excute lookup command on lookupd failed, msg: {}", e.getMessage());
            }
            sb.setLength(0);
        }

        return producers;
    }

}
