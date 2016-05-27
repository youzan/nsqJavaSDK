/**
 *
 */
package com.youzan.util;

import java.net.UnknownHostException;

import org.apache.commons.validator.routines.InetAddressValidator;

/**
 * IPv4 is 32 bit unsigned.
 * 
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 */
public final class IPUtil {

    public static long ipv4(String ipv4) throws UnknownHostException {
        if (!InetAddressValidator.getInstance().isValidInet4Address(ipv4)) {
            throw new UnknownHostException(ipv4);
        }
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
