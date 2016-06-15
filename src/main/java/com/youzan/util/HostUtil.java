/**
 * 
 */
package com.youzan.util;

import java.io.UncheckedIOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public final class HostUtil {
    private static final Logger logger = LoggerFactory.getLogger(HostUtil.class);

    public static final String getLocalIP() {
        try {
            final List<String> ips = new ArrayList<>(5);
            final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()
                            && address.getHostAddress().indexOf(":") == -1) {
                        ips.add(address.getHostAddress());
                    }
                }
            }
            if (!ips.isEmpty()) {
                ips.sort((s1, s2) -> s1.compareTo(s2));
                return ips.get(0);
            }

            logger.debug("Geting from localhost.");
            String local = InetAddress.getLocalHost().getHostAddress();

            if (local == null || "127.0.0.1".equals(local) || local.isEmpty()) {
                logger.error("Can't get the real IP!");
                throw new RuntimeException("We got one unexcepted Local IPv4. It is " + local);
            }
            return local;
        } catch (SocketException | UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }
}
