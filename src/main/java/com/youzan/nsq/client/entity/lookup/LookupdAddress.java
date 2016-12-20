package com.youzan.nsq.client.entity.lookup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 16/12/13.
 */
public class LookupdAddress extends AbstractLookupdAddress {
    private static ConcurrentHashMap<String, LookupdAddress> lookupMap = new ConcurrentHashMap<>();

    private AtomicLong refCounter;

    public LookupdAddress(String clusterId, String address) {
        super(clusterId, address);
        this.refCounter = new AtomicLong(0);
    }

    protected long updateRefCounter(int delta) {
        if(delta > 0) {
            return this.refCounter.incrementAndGet();
        } else if(delta < 0) {
            return this.refCounter.decrementAndGet();
        }
        return this.refCounter.get();
    }

    public long getReferenceCount() {
        return refCounter.get();
    }

    public long addReference(){
        synchronized (lookupMap) {
            if(lookupMap.containsValue(this))
                return this.updateRefCounter(1);
        }
        return -1;
    }

    public long removeReference() {
        synchronized (lookupMap) {
            if(lookupMap.containsValue(this)) {
                long cnt = this.updateRefCounter(-1);
                if (cnt <= 0) {
                    lookupMap.remove(this.getAddress());
                    return 0;
                }
            }
        }
        return -1;
    }

    public static LookupdAddress create(String clusterId, String address) {
        synchronized (lookupMap) {
            LookupdAddress aLookup = new LookupdAddress(clusterId, address);
            if(!aLookup.isValid())
                return null;
            if (!lookupMap.containsKey(address)) {
                lookupMap.put(address, aLookup);
            }else {
                aLookup = lookupMap.get(address);
            }
            return aLookup;
        }
    }
}
