package com.youzan.nsq.client.core.lookup;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

import com.youzan.nsq.client.entity.Address;

public class NSQLookupServiceImpl implements NSQLookupService {

    private static final long serialVersionUID = 1773482379917817275L;

    /**
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(List<String> addresses) {
        // TODO - implement NSQLookupServiceImpl.NSQLookupServiceImpl
        throw new UnsupportedOperationException();
    }

    /**
     * 
     * @param addresses
     */
    public NSQLookupServiceImpl(String addresses) {
        // TODO - implement NSQLookupServiceImpl.NSQLookupServiceImpl
        throw new UnsupportedOperationException();
    }

    /**
     * return ordered addresses
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

}
