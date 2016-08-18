package com.youzan.nsq.client.entity;

import java.util.BitSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Address implements java.io.Serializable, Comparable<Address> {
    private static final long serialVersionUID = -5091525135068063293L;

    private final String host;
    private final int port;

    //version number, used to specify old nsq and new
    private final String version;
    private Boolean isHA = null;

    /**
     * partition meter for recording partition ids which this broker (nsqd) presents, each bit presents
     * a partition id, for example:
     *
     * "00011011" means this broker address has partition 3,4,6,7 located
     */
    private volatile BitSet partitionsMeter = null;
    //TODO: overhead for sync, can we remove it? or need another structure for partition ids
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();;

    /**
     *
     * @param host
     * @param port
     */
    public Address(String host, String port, String version) {
        this.host = host;
        this.port = Integer.valueOf(port);
        this.version = version;
    }

    /**
     * @param host a {@link String}
     * @param port a integer number
     */
    public Address(String host, int port, String version) {
        this.host = host;
        this.port = port;
        this.version = version;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getVersion(){
        return this.version;
    }

    /**
     * check if version # ends with HA.*
     * @return
     */
    public boolean isHA(){
        if(null != isHA)
            return isHA;
        else
            if(this.version.contains("-HA."))
                return isHA = Boolean.TRUE;
            else return isHA = Boolean.FALSE;
    }

    /**
     * Add pass in partition ids into broker address. One partitionId belongs to one broker, while one broker
     * may have multi partition id attached.
     *
     *
     * @param partitionIds
     */
    public void addPartitionIds(int... partitionIds){
        lock.writeLock().lock();
        try {
            if (null == partitionsMeter)
                partitionsMeter = new BitSet();
            //as BitSet.set has not state, there is no need to sort pass in array first to improve performance
            for (int id : partitionIds)
                partitionsMeter.set(id);
        }finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * check if specified partition id is located in current address
     * @param partitionId partition id to check
     * @return true if current address has passed in partition id set, otherwise false
     */
    public boolean hasPartition(int partitionId){
        lock.readLock().lock();
        try {
            if (null == this.partitionsMeter)
                return false;
            else return partitionsMeter.get(partitionId);
        }finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    @Override
    public int compareTo(Address o2) {
        if (null == o2) {
            return 1;
        }
        final Address o1 = this;
        final int hostComparator = o1.host.compareTo(o2.getHost());
        return hostComparator == 0 ? o1.port - o2.port : hostComparator;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Address other = (Address) obj;
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)) {
            return false;
        }
        return port == other.port;
    }

}
