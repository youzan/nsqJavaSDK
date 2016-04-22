package com.youzan.nsq.client.remoting;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author caohaihong since 2015年10月29日 下午8:45:45
 */
public class NetworkUtils {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkUtils.class);

    public static enum StackType {
        IPv4, IPv6, Unknown
    }

    public static final String IPv4_SETTING = "java.net.preferIPv4Stack";
    public static final String IPv6_SETTING = "java.net.preferIPv6Addresses";
    public static final String NON_LOOPBACK_ADDRESS = "non_loopback_address";

    private final static InetAddress localAddress;

    static {
        InetAddress localAddressX;
        try {
            localAddressX = InetAddress.getLocalHost();
        } catch (Throwable e) {
            LOG.warn("failed to resolve local host, fallback to loopback", e);
            localAddressX = InetAddress.getLoopbackAddress();
        }
        localAddress = localAddressX;
    }

    public static boolean defaultReuseAddress() {
        return true;
    }

    public static boolean isIPv4() {
        return System.getProperty("java.net.preferIPv4Stack") != null
                && System.getProperty("java.net.preferIPv4Stack").equals("true");
    }

    public static InetAddress getIPv4Localhost() throws UnknownHostException {
        return getLocalhost(StackType.IPv4);
    }

    public static InetAddress getIPv6Localhost() throws UnknownHostException {
        return getLocalhost(StackType.IPv6);
    }

    public static InetAddress getLocalAddress() {
        return localAddress;
    }

    public static String getLocalHostName(String defaultHostName) {
        if (localAddress == null) {
            return defaultHostName;
        }
        String hostName = localAddress.getHostName();
        if (hostName == null) {
            return defaultHostName;
        }
        return hostName;
    }

    public static String getLocalHostAddress(String defaultHostAddress) {
        if (localAddress == null) {
            return defaultHostAddress;
        }
        String hostAddress = localAddress.getHostAddress();
        if (hostAddress == null) {
            return defaultHostAddress;
        }
        return hostAddress;
    }

    public static InetAddress getLocalhost(StackType ip_version) throws UnknownHostException {
        if (ip_version == StackType.IPv4)
            return InetAddress.getByName("127.0.0.1");
        else
            return InetAddress.getByName("::1");
    }

    /**
     * Returns the first non-loopback address on any interface on the current
     * host.
     * 
     * @param ip_version
     *            Constraint on IP version of address to be returned, 4 or 6
     */
    public static InetAddress getFirstNonLoopbackAddress(StackType ip_version) throws SocketException {
        InetAddress address;
        for (NetworkInterface intf : getInterfaces()) {
            try {
                if (!intf.isUp() || intf.isLoopback())
                    continue;
            } catch (Exception e) {
                // might happen when calling on a network interface that does
                // not exists
                continue;
            }
            address = getFirstNonLoopbackAddress(intf, ip_version);
            if (address != null) {
                return address;
            }
        }

        return null;
    }

    private static List<NetworkInterface> getInterfaces() throws SocketException {
        Enumeration<NetworkInterface> intfs = NetworkInterface.getNetworkInterfaces();

        List<NetworkInterface> intfsList = new ArrayList<NetworkInterface>();
        while (intfs.hasMoreElements()) {
            intfsList.add((NetworkInterface) intfs.nextElement());
        }

        return intfsList;
    }

    /**
     * Returns the first non-loopback address on the given interface on the
     * current host.
     * 
     * @param intf
     *            the interface to be checked
     * @param ipVersion
     *            Constraint on IP version of address to be returned, 4 or 6
     */
    public static InetAddress getFirstNonLoopbackAddress(NetworkInterface intf, StackType ipVersion)
            throws SocketException {
        if (intf == null)
            throw new IllegalArgumentException("Network interface pointer is null");

        for (Enumeration<InetAddress> addresses = intf.getInetAddresses(); addresses.hasMoreElements();) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if (!address.isLoopbackAddress()) {
                if ((address instanceof Inet4Address && ipVersion == StackType.IPv4)
                        || (address instanceof Inet6Address && ipVersion == StackType.IPv6))
                    return address;
            }
        }
        return null;
    }

    /**
     * Returns the first address with the proper ipVersion on the given
     * interface on the current host.
     * 
     * @param intf
     *            the interface to be checked
     * @param ipVersion
     *            Constraint on IP version of address to be returned, 4 or 6
     */
    public static InetAddress getFirstAddress(NetworkInterface intf, StackType ipVersion) throws SocketException {
        if (intf == null)
            throw new IllegalArgumentException("Network interface pointer is null");

        for (Enumeration<InetAddress> addresses = intf.getInetAddresses(); addresses.hasMoreElements();) {
            InetAddress address = (InetAddress) addresses.nextElement();
            if ((address instanceof Inet4Address && ipVersion == StackType.IPv4)
                    || (address instanceof Inet6Address && ipVersion == StackType.IPv6))
                return address;
        }
        return null;
    }

    /**
     * A function to check if an interface supports an IP version (i.e has
     * addresses defined for that IP version).
     * 
     * @param intf
     * @return
     */
    public static boolean interfaceHasIPAddresses(NetworkInterface intf, StackType ipVersion) throws SocketException,
            UnknownHostException {
        boolean supportsVersion = false;
        if (intf != null) {
            // get all the InetAddresses defined on the interface
            Enumeration<InetAddress> addresses = intf.getInetAddresses();
            while (addresses != null && addresses.hasMoreElements()) {
                // get the next InetAddress for the current interface
                InetAddress address = (InetAddress) addresses.nextElement();

                // check if we find an address of correct version
                if ((address instanceof Inet4Address && (ipVersion == StackType.IPv4))
                        || (address instanceof Inet6Address && (ipVersion == StackType.IPv6))) {
                    supportsVersion = true;
                    break;
                }
            }
        } else {
            throw new UnknownHostException("network interface not found");
        }
        return supportsVersion;
    }

    /**
     * Tries to determine the type of IP stack from the available interfaces and
     * their addresses and from the system properties (java.net.preferIPv4Stack
     * and java.net.preferIPv6Addresses)
     * 
     * @return StackType.IPv4 for an IPv4 only stack, StackYTypeIPv6 for an IPv6
     *         only stack, and StackType.Unknown if the type cannot be detected
     */
    public static StackType getIpStackType() {
        boolean isIPv4StackAvailable = isStackAvailable(true);
        boolean isIPv6StackAvailable = isStackAvailable(false);

        // if only IPv4 stack available
        if (isIPv4StackAvailable && !isIPv6StackAvailable) {
            return StackType.IPv4;
        }
        // if only IPv6 stack available
        else if (isIPv6StackAvailable && !isIPv4StackAvailable) {
            return StackType.IPv6;
        }
        // if dual stack
        else if (isIPv4StackAvailable && isIPv6StackAvailable) {
            // get the System property which records user preference for a stack
            // on a dual stack machine
            if (Boolean.getBoolean(IPv4_SETTING)) // has preference over
                                                  // java.net.preferIPv6Addresses
                return StackType.IPv4;
            if (Boolean.getBoolean(IPv6_SETTING))
                return StackType.IPv6;
            return StackType.IPv6;
        }
        return StackType.Unknown;
    }

    public static boolean isStackAvailable(boolean ipv4) {
        Collection<InetAddress> allAddrs = getAllAvailableAddresses();
        for (InetAddress addr : allAddrs)
            if (ipv4 && addr instanceof Inet4Address || (!ipv4 && addr instanceof Inet6Address))
                return true;
        return false;
    }

    /**
     * Returns all the available interfaces, including first level sub
     * interfaces.
     */
    public static List<NetworkInterface> getAllAvailableInterfaces() throws SocketException {
        List<NetworkInterface> allInterfaces = new ArrayList<>();
        for (Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces(); interfaces
                .hasMoreElements();) {
            NetworkInterface intf = interfaces.nextElement();
            allInterfaces.add(intf);

            Enumeration<NetworkInterface> subInterfaces = intf.getSubInterfaces();
            if (subInterfaces != null && subInterfaces.hasMoreElements()) {
                while (subInterfaces.hasMoreElements()) {
                    allInterfaces.add(subInterfaces.nextElement());
                }
            }
        }
        return allInterfaces;
    }

    public static Collection<InetAddress> getAllAvailableAddresses() {
        Set<InetAddress> retval = new TreeSet<InetAddress>();
        try {
            for (NetworkInterface intf : getInterfaces()) {
                Enumeration<InetAddress> addrs = intf.getInetAddresses();
                while (addrs.hasMoreElements())
                    retval.add(addrs.nextElement());
            }
        } catch (SocketException e) {
            LOG.warn("Failed to derive all available interfaces", e);
        }

        return retval;
    }
}
