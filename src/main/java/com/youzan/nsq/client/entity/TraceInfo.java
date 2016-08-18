package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.network.frame.NSQFrame;

import java.nio.ByteBuffer;

/**
 * Created by lin on 16/9/8.
 */
public class TraceInfo {
    final private byte[] internalIdByte = new byte[8];
    private Long internalId = null;
    final private byte[] traceIdByte = new byte[8];
    private Long traceId = null;
    final private byte[] diskQueueOffsetByte = new byte[8];
    private Long diskQueueOffset = null;
    final private byte[] internalDiskQueueDataSizeByte = new byte[4];
    private Integer internalDiskQueueDataSize = null;
    private String traceInfoStr= null;

    public TraceInfo(NSQFrame frame){
        ByteBuffer buf = ByteBuffer.allocate(frame.getData().length);
        buf.put(frame.getData());
        buf.rewind();
        //skip OK(2 bytes)
        buf.get(new byte[2]);
        buf.get(internalIdByte);
        buf.get(traceIdByte);
        buf.get(diskQueueOffsetByte);
        buf.get(internalDiskQueueDataSizeByte);
    }

    public long getInternalId(){
        if(null == internalId) {
            ByteBuffer buf = ByteBuffer.wrap(this.internalIdByte);
            internalId = buf.getLong();
        }
        return this.internalId;
    }

    public long getTraceId(){
        if(null == this.traceId) {
            ByteBuffer buf = ByteBuffer.wrap(this.traceIdByte);
            traceId = buf.getLong();
        }
        return this.traceId;
    }

    public long getDiskQueueOffset(){
        if(null == this.diskQueueOffset){
            ByteBuffer buf = ByteBuffer.wrap(this.diskQueueOffsetByte);
            this.diskQueueOffset = buf.getLong();
        }
        return this.diskQueueOffset;
    }

    public int getInternalDiskQueueDataSize(){
        if(null == this.internalDiskQueueDataSize){
            ByteBuffer buf = ByteBuffer.wrap(this.internalDiskQueueDataSizeByte);
            this.internalDiskQueueDataSize = buf.getInt();
        }
        return this.internalDiskQueueDataSize;
    }

    public String toString(){
        if(null == this.traceInfoStr) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\n");
            sb.append("InternalID: ").append(this.getInternalId()).append(";\n")
                    .append("TraceID: ").append(this.getTraceId()).append(";\n")
                    .append("DiskQueueOffset: ").append(this.getDiskQueueOffset()).append(";\n")
                    .append("InternalDiskQueueDateSize: ").append(this.getInternalDiskQueueDataSize()).append(";\n");
            sb.append("}\n");
            traceInfoStr = sb.toString();
        }
        return traceInfoStr;
    }
}
