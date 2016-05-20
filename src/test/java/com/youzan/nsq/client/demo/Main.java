/**
 * 
 */
package com.youzan.nsq.client.demo;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        final byte delimiter = '\n';
        System.out.println(delimiter);
        final byte[] utf = "\n".getBytes("UTF-8");
        System.out.println("UTF-8: " + utf);

        final byte[] ascii = "\n".getBytes("US-ASCII");
        System.out.println("ASCII: " + ascii);

        System.out.println(String.valueOf(delimiter) + "   char:" + (char) delimiter);
    }
}
