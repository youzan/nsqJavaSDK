package com.youzan.nsq.client;

import com.youzan.nsq.client.entity.Message;

/**
 * Message receipt for publish response, for normal publish(PUB),
 * it contains: topic name, partition number, nsqd address(host:port);
 * if current receipt is for trace publish(PUB_TARCE), when {@link Message#traced()},
 * following info contains, besides topic and nsqd info mentioned above:
 *
 * [internalID]
 * [traceID]
 * [diskQueueOffset]
 * [diskQueueDataSize]
 */
public class MessageReceipt implements MessageMetadata {
    private long internalID;
    private long traceID;
    private long diskQueueOffset=-1l;
    private int diskQueueSize=-1;
    private String topicName;
    private int partition;
    private String nsqdAddr;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getNsqdAddr() {
        return nsqdAddr;
    }

    public void setNsqdAddr(String nsqdAddr) {
        this.nsqdAddr = nsqdAddr;
    }

    public MessageReceipt() {

    }

    public void setInternalID(long internalID) {
        this.internalID = internalID;
    }

    public long getInternalID() {
        return internalID;
    }

    public long getTraceID() {
        return traceID;
    }

    public void setTraceID(long traceID) {
        this.traceID = traceID;
    }

    public long getDiskQueueOffset() {
        return diskQueueOffset;
    }

    public void setDiskQueueOffset(long diskQueueOffset) {
        this.diskQueueOffset = diskQueueOffset;
    }

    public int getDiskQueueSize() {
        return diskQueueSize;
    }

    public void setDiskQueueSize(int diskQueueSize) {
        this.diskQueueSize = diskQueueSize;
    }

    @Override
    public String toMetadataStr() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().toString() + " meta-data:").append("\n");
        sb.append("\t[topic]:\t").append(topicName).append(", ").append(partition).append("\n");
        sb.append("\t[nsqdAddr]:\t").append(nsqdAddr).append("\n");
        sb.append("\t[internalID]:\t").append(internalID).append("\n");
        sb.append("\t[traceID]:\t").append(traceID).append("\n");
        sb.append("\t[diskQueueOffset]:\t").append(diskQueueOffset).append("\n");
        sb.append("\t[diskQueueDataSize]:\t").append(diskQueueSize).append("\n");
        sb.append(this.getClass().toString() + " end.");
        return sb.toString();
    }
}
