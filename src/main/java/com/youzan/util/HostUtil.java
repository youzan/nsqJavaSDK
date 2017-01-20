/**
 *
 */
package com.youzan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public final class HostUtil {
    private static final Logger logger = LoggerFactory.getLogger(HostUtil.class);

    public static String getLocalIP() throws IOException {
        try {
            final List<String> ips = new ArrayList<>(5);
            final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()
                            && !address.getHostAddress().contains(":")) {
                        ips.add(address.getHostAddress());
                    }
                }
            }
            if (!ips.isEmpty()) {
                // JDK8
                // (s1, s2) -> s1.compareTo(s2)
                Collections.sort(ips);
                return ips.get(0);
            }

            logger.debug("Have got from localhost.");
            String local = InetAddress.getLocalHost().getHostAddress();

            if (local == null || "127.0.0.1".equals(local) || local.isEmpty()) {
                logger.error("Can't get the real IP!");
                throw new RuntimeException("We got one unexpected Local IPv4. It is " + local);
            }
            return local;
        } catch (SocketException | UnknownHostException e) {
            throw new IOException(e);
        }
    }

    public static String getHostname() {
        String hostname = null;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("Could not get host name for local host.", e);
        }finally {
            return hostname;
        }
    }
}
