package com.youzan.nsq.client;

import com.youzan.nsq.client.core.NSQConnection;
import com.youzan.nsq.client.core.command.Sub;
import com.youzan.nsq.client.core.command.SubOrdered;
import com.youzan.nsq.client.entity.Address;
import com.youzan.nsq.client.entity.NSQAdvMessage;
import com.youzan.nsq.client.entity.NSQMessage;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.exception.NSQException;
import com.youzan.nsq.client.network.frame.MessageFrame;
import com.youzan.nsq.client.network.frame.NSQFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by lin on 16/9/12.
 */
public abstract class AbstractConsumer implements Consumer, HasSubscribeStatus {

    private static Logger logger = LoggerFactory.getLogger(AbstractConsumer.class);
    protected SubCmdType subStatus = SubCmdType.SUB;
    private Object traceLock = new Object();
    private AtomicLong latestInternalID = null;
    private AtomicLong latestDiskQueueOffset = null;

    @Override
    public abstract void subscribe(String... topics);

    @Override
    public abstract void subscribe(int partitionId, String... topics);

    @Override
    public abstract void start() throws NSQException;

    @Override
    public abstract void finish(NSQMessage message) throws NSQException;

    @Override
    public abstract void setAutoFinish(boolean autoFinish);

    @Override
    public abstract void close();

    @Override
    public abstract void incoming(NSQFrame frame, NSQConnection conn) throws NSQException;

    @Override
    public abstract void backoff(NSQConnection conn);

    @Override
    public abstract boolean validateHeartbeat(NSQConnection conn);

    @Override
    public abstract Set<NSQConnection> clearDataNode(Address address);

    @Override
    public abstract void close(NSQConnection conn);

    /**
     * process message in abstract consumer, record internal ID and diskQueueOffset
     * @param message
     * @param connection
     */
    void processMessage(final NSQMessage message, final NSQConnection connection){
        if(logger.isDebugEnabled()){
            logger.debug(message.toString());
        }


        if(message instanceof NSQAdvMessage){
            NSQAdvMessage msg = (NSQAdvMessage) message;
            long diskQueueOffset = msg.getDiskQueueOffset();
            long internalID = message.getInternalID();
            synchronized(traceLock) {
                long current = this.latestInternalID.get();
                if(internalID <= current){
                    //there is a problem
                    logger.error("Internal ID in current message is smaller than what has received. Latest internal ID: {}, Current internal ID: {}.", current, internalID);
                }

                current = this.latestDiskQueueOffset.get();
                if(diskQueueOffset <= current){
                    //there is another problem
                    logger.error("Disk queue offset in current message is smaller than what has received. Latest disk queue offset ID: {}, Current disk queue offset: {}.", current, diskQueueOffset);
                }
                //update both values
                this.latestInternalID.set(internalID);
                this.latestDiskQueueOffset.set(diskQueueOffset);
            }
        }
    }

    public synchronized SubCmdType getSubscribeStatus() {
        return this.subStatus;
    }

    public NSQMessage createNSQMessage(final MessageFrame msgFrame, final NSQConnection conn, SubCmdType subType){
        switch(subType){
            case SUB_ORDERED: {
                return new NSQAdvMessage(msgFrame.getTimestamp(), msgFrame.getAttempts(), msgFrame.getMessageID(),
                        msgFrame.getInternalID(), msgFrame.getTractID(), msgFrame.getDiskQueueOffset(), msgFrame.getDiskQueueDataSize(), msgFrame.getMessageBody(), conn.getAddress(), conn.getId());
            }
            default: {
                return new NSQMessage(msgFrame.getTimestamp(), msgFrame.getAttempts(), msgFrame.getMessageID(),
                        msgFrame.getInternalID(), msgFrame.getTractID(), msgFrame.getMessageBody(), conn.getAddress(), conn.getId());
            }
        }
    }
}
