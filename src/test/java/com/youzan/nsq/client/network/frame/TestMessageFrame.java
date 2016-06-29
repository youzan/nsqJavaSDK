package com.youzan.nsq.client.network.frame;

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

        byte[] timestamp = new byte[8];
        byte[] attempts = new byte[2];
        byte[] messageID = new byte[16];
        byte[] data = new byte[size];
        byte[] actualData = frame.getData();
    }

}
