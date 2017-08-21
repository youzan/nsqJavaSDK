package com.youzan.nsq.client.core.command;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.youzan.nsq.client.entity.Message;
import com.youzan.util.SystemUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.IllegalFormatException;

/**
 * PubExt command
 * Created by lin on 17/7/29.
 */
public class PubExt extends Pub {
    private byte[] jsonHeaderBytes;
    public static final String CLIENT_TAG_KEY = "##client_dispatch_tag";
    public static final String TRACE_ID_KEY = "##trace_id";

    /**
     * @param msg message object
     */
    public PubExt(final Message msg, boolean trace) throws IllegalFormatException {
        super(msg);
        String clientTag = msg.getDesiredTag();
        boolean jsonHeaderNeeded = (null != clientTag && !clientTag.isEmpty()) || (trace);

        Object jsonObj = msg.getJsonHeaderExt();
        ObjectNode jsonHeaderExt = null;
        //create one json
        if (null == jsonObj && jsonHeaderNeeded) {
            jsonHeaderExt = SystemUtil.getObjectMapper().createObjectNode();
            if(null != clientTag)
                jsonHeaderExt.put(CLIENT_TAG_KEY, clientTag);
            if(trace)
                jsonHeaderExt.put(TRACE_ID_KEY, msg.getTraceIDStr());
        } else if (null != jsonObj) {
            //parse message json header ext
            try {
                String jsonStr = SystemUtil.getObjectMapper().writeValueAsString(jsonObj);
                JsonNode json = SystemUtil.getObjectMapper().readTree(jsonStr);
                //override client tag
                if(json.isObject()) {
                    jsonHeaderExt = (ObjectNode)json;
                    if(null != clientTag)
                        jsonHeaderExt.put(CLIENT_TAG_KEY, clientTag);
                    if(trace)
                        jsonHeaderExt.put(TRACE_ID_KEY, msg.getTraceIDStr());
                } else {
                    //throw error
                    throw new IllegalStateException("Invalid json header format, pass in json root is not object.");
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not parse json header.");
            }
        } else {
            //throw error
            throw new IllegalStateException("Invalid json header format. Json header not specified.");
        }
        try {
            this.jsonHeaderBytes = SystemUtil.getObjectMapper().writeValueAsBytes(jsonHeaderExt);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Fail to convert object node to string.");
        }
    }

    @Override
    public byte[] getBytes() {
        if(null == bytes){
            byte[] header = this.getHeader().getBytes(NSQCommand.DEFAULT_CHARSET);
            byte[] jsonHeaderBytes = this.jsonHeaderBytes;
            byte[] body = this.getBody().get(0);
            ByteBuffer buf = ByteBuffer.allocate(header.length + 4/*total length*/ + 2/*json header length*/ + jsonHeaderBytes.length + body.length);

            buf.put(header)
                    .putInt(2 + jsonHeaderBytes.length + body.length)
                    .putShort((short) jsonHeaderBytes.length)
                    .put(jsonHeaderBytes)
                    .put(body);
            bytes = buf.array();
        }
        return bytes;
    }

    @Override
    public String getHeader() {
        return String.format("PUB_EXT %s%s\n", topic.getTopicText(), this.getPartitionStr());
    }
}
