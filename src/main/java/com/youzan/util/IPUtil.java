/**
 *
 */
package com.youzan.util;

import java.net.UnknownHostException;

/**
 * IPv4 is 32 bit unsigned.
 * 
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 * 
 */
public final class IPUtil {

    public static long ipv4(String ipv4) throws UnknownHostException {
        long result = 0L;
        String[] bs = ipv4.split("\\.");
        result |= Long.parseLong(bs[0]) << 24;
        result |= Long.parseLong(bs[1]) << 16;
        result |= Long.parseLong(bs[2]) << 8;
        result |= Long.parseLong(bs[3]);
        return result;
    }

    public static String ipv4(long ipv4) throws UnknownHostException {
        if (ipv4 < 0) {
            throw new UnknownHostException(String.valueOf(ipv4));
        }
        final StringBuffer result = new StringBuffer(15);
        result.append((ipv4 >> 24) & 0xFF);
        result.append(".");
        result.append((ipv4 >> 16) & 0xFF);
        result.append(".");
        result.append((ipv4 >> 8) & 0xFF);
        result.append(".");
        result.append(ipv4 & 0xFF);
        return result.toString();
    }
}
