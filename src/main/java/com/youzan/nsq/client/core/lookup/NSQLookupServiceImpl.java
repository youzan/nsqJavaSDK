package com.youzan.nsq.client.core.lookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;

public class NSQLookupServiceImpl implements NSQLookupService {

    private static final long serialVersionUID = 1773482379917817275L;

    private final List<String> addresses;

    /**
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(List<String> addresses) {
        this.addresses = addresses;
    }

    /**
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(String addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("addresses is blank!");
        }
        String[] tmp = addresses.split(",");
        if (tmp != null) {
            this.addresses = new ArrayList<>(tmp.length);
            for (String addr : tmp) {
                this.addresses.add(addr);
            }
        } else {
            this.addresses = new ArrayList<>(0);
        }
    }

    /**
     * 
     * @param topic
     * @param writable
     */
    @Override
    public SortedSet<Address> lookup(String topic, boolean writable) {
        // TODO - implement NSQLookupServiceImpl.lookup
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean save() {
        // TODO - implement NSQLookupServiceImpl.save
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean load() {
        // TODO - implement NSQLookupServiceImpl.load
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * @return the serialversionuid
     */
    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    /**
     * @return the addresses
     */
    public List<String> getAddresses() {
        return addresses;
    }

}
