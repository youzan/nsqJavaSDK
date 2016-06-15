/**
 * 
 */
package com.youzan.nsq.client.demo;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * @param args
     * @throws UnsupportedEncodingException
     */
    public static void main(String[] args) throws UnsupportedEncodingException {
        byte[] bs = new byte[] { (byte) 0xFF, 0x00, (byte) 0x01 };

        System.out.println("bytes: " + 0x00 + " " + 0xFF);
        System.out.println("bytes: " + 0x00 + " " + 0xFFFF);
        final byte delimiter = '\n';
        System.out.println(delimiter);
        final byte[] utf = "\n".getBytes("UTF-8");
        System.out.println("UTF-8: " + utf);

        final byte[] ascii = "\n".getBytes("US-ASCII");
        System.out.println("ASCII: " + ascii);

        System.out.println(String.valueOf(delimiter) + "   char:" + (char) delimiter);
    }

    @DataProvider(name = "db")
    public Object[][] data() {
        Object[][] o = new Object[4][2];
        o[0] = new Object[] { new byte[] { 0x00, 0x00 }, 0 };
        o[1] = new Object[] { new byte[] { 0x00, 0x01 }, 1 };
        o[2] = new Object[] { new byte[] { 0x01, 0x00 }, 256 };
        o[3] = new Object[] { new byte[] { (byte) 0xFF, (byte) 0xFF }, 65535 };
        return o;
    }

    @Test()
    public void bytes() {
        System.out.println(0x00 + " " + 0xFF);
    }

    int toUnsignedShort(byte[] bytes) {
        return (bytes[0] << 8 | bytes[1] & 0xFF) & 0xFFFF;
    }
}
