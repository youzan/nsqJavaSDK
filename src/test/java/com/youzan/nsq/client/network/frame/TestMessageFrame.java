package com.youzan.nsq.client.network.frame;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMessageFrame {
    @Test
    public void newInstance() {
        MessageFrame frame = new MessageFrame();
    }

    @Test
    public void setData() {
        MessageFrame frame = new MessageFrame();
        int size = 10;
        byte[] bytes = new byte[size];
        frame.setSize(10);
        frame.setData(bytes);
        byte[] actual = frame.getData();
        Assert.assertEquals(actual, bytes);
    }

}
