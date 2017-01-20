package com.youzan.nsq.client.network.frame;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * http://nsq.io/clients/tcp_protocol_spec.html#data-format
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public class TestMessageFrame {
    private Random _r = new Random();

    @Test
    public void newInstance() {
        MessageFrame frame = new MessageFrame();
    }

    @Test
    public void setData() {
        MessageFrame frame = new MessageFrame();
        int size = 100;
        int dataSize = size - 4;
        // size + type + data
        // timestamp + attempts + ID + messageBody

        byte[] timestamp = new byte[8];
        byte[] attempts = new byte[2];
        byte[] messageID = new byte[16];
        byte[] messageBody = new byte[dataSize - 8 - 2 - 16];
        _r.nextBytes(timestamp);
        _r.nextBytes(attempts);
        _r.nextBytes(messageID);
        _r.nextBytes(messageBody);

        ByteBuffer bb = java.nio.ByteBuffer.allocate(dataSize);
        bb.put(timestamp);
        bb.put(attempts);
        bb.put(messageID);
        bb.put(messageBody);
        byte[] data = bb.array();

        frame.setSize(dataSize);
        frame.setData(data);

        Assert.assertEquals(frame.getTimestamp(), timestamp);
        Assert.assertEquals(frame.getAttempts(), attempts);
        Assert.assertEquals(frame.getMessageID(), messageID);
        Assert.assertEquals(frame.getMessageBody(), messageBody);
    }

    @Test(expectedExceptions = Exception.class)
    public void dataException() {
        MessageFrame frame = new MessageFrame();
        int size = 29;
        int dataSize = size - 4;
        // size + type + data
        // timestamp + attempts + ID + messageBody

        byte[] timestamp = new byte[8];
        byte[] attempts = new byte[2];
        byte[] messageID = new byte[16];
        byte[] messageBody = new byte[dataSize - 8 - 2 - 16];
        _r.nextBytes(timestamp);
        _r.nextBytes(attempts);
        _r.nextBytes(messageID);
        _r.nextBytes(messageBody);

        ByteBuffer bb = java.nio.ByteBuffer.allocate(dataSize);
        bb.put(timestamp);
        bb.put(attempts);
        bb.put(messageID);
        bb.put(messageBody);
        byte[] data = bb.array();

        frame.setSize(dataSize);
        frame.setData(data);

    }

    @Test
    public void dataBoundary() {
        MessageFrame frame = new MessageFrame();
        int size = 30;
        int dataSize = size - 4;
        // size + type + data
        // timestamp + attempts + ID + messageBody

        byte[] timestamp = new byte[8];
        byte[] attempts = new byte[2];
        byte[] messageID = new byte[16];
        byte[] messageBody = new byte[dataSize - 8 - 2 - 16];
        _r.nextBytes(timestamp);
        _r.nextBytes(attempts);
        _r.nextBytes(messageID);
        _r.nextBytes(messageBody);

        ByteBuffer bb = java.nio.ByteBuffer.allocate(dataSize);
        bb.put(timestamp);
        bb.put(attempts);
        bb.put(messageID);
        bb.put(messageBody);
        byte[] data = bb.array();

        frame.setSize(dataSize);
        frame.setData(data);
    }

}
