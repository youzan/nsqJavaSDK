package com.youzan.nsq.client.entity.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.nsq.client.entity.*;
import com.youzan.nsq.client.exception.NSQLookupException;
import com.youzan.nsq.client.exception.NSQProducerNotFoundException;
import com.youzan.nsq.client.exception.NSQTopicNotFoundException;
import com.youzan.util.IOUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.net.URL;
import java.util.*;

/**
 * Created by lin on 16/12/13.
 */
public class NSQLookupdAddresses extends AbstractLookupdAddresses {
    private static final Logger logger = LoggerFactory.getLogger(NSQLookupdAddresses.class);
    private static final String HTTP_PRO_HEAD = "http://";
    private static final String BROKER_QUERY_URL_WRITE = "%s/lookup?topic=%s&access=%s&metainfo=true";
    private static final String BROKER_QUERY_URL_READ = "%s/lookup?topic=%s&access=%s&metainfo=true";

    protected int prefactor = 0;

    public NSQLookupdAddresses(List<String> clusterIds, List<String> addresses) {
        super(clusterIds, addresses);
    }

    public static NSQLookupdAddresses create(List<String> clusterIds, List<String> lookupdAddrs) {
        return new NSQLookupdAddresses(clusterIds, lookupdAddrs);
    }

    public static NSQLookupdAddresses create(List<String> preClusterIds, List<String> previousLookupds, List<String> curClusterIds, List<String> currentLookupds, int currentFactor) {
        return new NSQLookupdAddressesPair(preClusterIds, previousLookupds, curClusterIds, currentLookupds, currentFactor);
    }

    /**
     * return factor of previous lookup address(in %)
     * @return preFactor percent factor in previous node.
     */
    public int getPreFactor(){
        return this.prefactor;
    }


    /**
     * lookup NSQ nodes from current {@link NSQLookupdAddresses}, return mapping from cluster ID to partition nodes
     * @param topic     topic for lookup
     * @param writable  {@link Boolean#TRUE} for write access, otherwise {@link Boolean#FALSE}.
     * @return partitionsSelector partitions selector containing nsq data nodes for current NSQ(and optional previous NSQ, in a merge NSQ)
     * @throws NSQLookupException
     *      exception during lookup process
     * @throws NSQProducerNotFoundException
     *      exception raised when NSQd not found
     * @throws NSQTopicNotFoundException
     *      exception when target topic not found
     */
    public IPartitionsSelector lookup(final String topic, boolean writable) throws NSQLookupException, NSQProducerNotFoundException, NSQTopicNotFoundException {
        if (null == topic || topic.isEmpty()) {
            throw new NSQLookupException("Your input topic is blank!");
        }
        /*
         * It is unnecessary to use Atomic/Lock for the variable
         */
        List<String> lookups = this.getAddresses();
        List<Partitions> allPartitions = new ArrayList<>();
        for(String aLookup:lookups) {
            try {
                Partitions aPartitions = lookup(aLookup, topic, writable);
                if (null != aPartitions)
                    allPartitions.add(aPartitions);
            }catch (IOException e) {
                final String tip = "SDK can't get the right lookup info. " + aLookup;
                throw new NSQLookupException(tip, e);
            }
        }
        if(allPartitions.size() < 1)
            return null;
        IPartitionsSelector ps = new SimplePartitionsSelector(allPartitions);
        return ps; // maybe it is empty
    }

    class AddressCompatibility implements Comparable<AddressCompatibility>{
        private Address addr;

        public AddressCompatibility(final Address addr) {
           this.addr = addr;
        }

        @Override
        public int compareTo(AddressCompatibility o2) {
            if (null == o2) {
                return 1;
            }
            final AddressCompatibility o1 = this;
            final int hostComparator = o1.addr.getHost().compareTo(o2.addr.getHost());
            return hostComparator == 0 ? o1.addr.getPort() - o2.addr.getPort() : hostComparator;
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
            AddressCompatibility other = (AddressCompatibility) obj;
            if (addr.getHost() == null) {
                if (other.addr.getHost() != null) {
                    return false;
                }
            } else if (!addr.getHost().equals(other.addr.getHost())) {
                return false;
            }
            return addr.getPort() == other.addr.getPort();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((addr.getHost() == null) ? 0 : addr.getHost().hashCode());
            result = prime * result + addr.getPort();
            return result;
        }
    }

    protected Partitions lookup(String lookupdAddress, final String topic, boolean writable) throws IOException, NSQProducerNotFoundException, NSQTopicNotFoundException {
        if(!lookupdAddress.startsWith(HTTP_PRO_HEAD))
            lookupdAddress = HTTP_PRO_HEAD + lookupdAddress;
        String urlFormat = writable ? BROKER_QUERY_URL_WRITE : BROKER_QUERY_URL_READ ;
        final String url = String.format(urlFormat, lookupdAddress, topic, writable ? "w" : "r");
        logger.debug("Begin to lookup some DataNodes from URL {}", url);
        Partitions aPartitions = new Partitions(topic);
        JsonNode rootNode;
        try {
            rootNode = IOUtil.readFromUrl(new URL(url));
        }catch(FileNotFoundException topicNotFoundExp) {
            throw new NSQTopicNotFoundException("Topic not found for " + topic + ", with query: " + url + ". topic or channel within may NOT exist", topic, topicNotFoundExp);
        }
        long start = 0L;
        if(logger.isDebugEnabled())
            start = System.currentTimeMillis();

        //check if producers exists, if not, it could be a no channel exception
        final JsonNode producers = rootNode.get("producers");
        if(null == producers || producers.size() == 0){
            throw new NSQProducerNotFoundException("No NSQd producer node return in lookup response. NSQd may not ready at this moment. " + topic);
        }
        final JsonNode partitions = rootNode.get("partitions");
        Map<Integer, SoftReference<Address>> partitionId2Ref;
        List<Address> partitionedDataNodes;
        Set<AddressCompatibility> partitionNodeSet = new HashSet<>();

        boolean isExtendable = false;
        int partitionCount = 0;

        JsonNode metaInfo = rootNode.get("meta");
        if (null != metaInfo) {
            JsonNode isExtendableJson = metaInfo.get("extend_support");
            if (null != isExtendableJson)
                isExtendable = isExtendableJson.asBoolean();

            //fetch meta info if it is writable
            if (writable) {
                partitionCount = metaInfo.get("partition_num").asInt();
            }
        }

        List<Address> unPartitionedDataNodes = null;
        if (null != partitions) {

            partitionId2Ref = new HashMap<>();
            partitionedDataNodes = new ArrayList<>();

            Iterator<String> irt = partitions.fieldNames();
            while (irt.hasNext()) {
                String parId = irt.next();
                int parIdInt = Integer.valueOf(parId);
                JsonNode partition = partitions.get(parId);
                final Address address = createAddress(topic, parIdInt, partition, isExtendable);
                if(parIdInt >= 0) {
                    partitionedDataNodes.add(address);
                    partitionNodeSet.add(new AddressCompatibility(address));
                    partitionId2Ref.put(parIdInt, new SoftReference<>(address));
                    if(!writable)
                        partitionCount++;
                }
            }
            aPartitions.updatePartitionDataNode(partitionId2Ref, partitionedDataNodes, partitionCount);
            if(logger.isDebugEnabled()){
                logger.debug("SDK took {} mill sec to create mapping for partition.", (System.currentTimeMillis() - start));
            }
        }

        //producers part in json
        for (JsonNode node : producers) {
            //for old NSQd partition, we set partition as -1
            final Address address = createAddress(topic, -1, node, false);
            if(!partitionNodeSet.contains(new AddressCompatibility(address))) {
                if(null == unPartitionedDataNodes)
                    unPartitionedDataNodes = new ArrayList<>();
                unPartitionedDataNodes.add(address);
            }
        }

        if(null != unPartitionedDataNodes)
            aPartitions.updateUnpartitionedDataNodea(unPartitionedDataNodes);
        partitionNodeSet.clear();
        if(logger.isDebugEnabled())
            logger.debug("The server response info after looking up some DataNodes: {}", rootNode.toString());

        if(!aPartitions.hasAnyDataNodes()){
            logger.error("Could not find any NSQ data node from lookup address {}", lookupdAddress);
            return null;
        }
        return aPartitions;
    }

    /**
     * create nsq broker Address, using pass in JsonNode,
     * specidied key in function will be used to extract
     * field to construct Address
     * @return Address nsq broker Address
     */
    private Address createAddress(final String topic, int partition, JsonNode node, boolean extend){
        final String host = node.get("broadcast_address").asText();
        final int port = node.get("tcp_port").asInt();
        final String version = node.get("version").asText();
        return new Address(host, port, version, topic, partition, extend);
    }
}


