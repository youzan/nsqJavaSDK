package com.youzan.nsq.client.core;

import com.youzan.nsq.client.IConsumeInfo;
import com.youzan.nsq.client.core.command.Rdy;
import com.youzan.nsq.client.entity.Address;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lin on 17/6/26.
 */
public class ConnectionManager {
    private final static Logger logger = LoggerFactory.getLogger(ConnectionManager.class.getName());
    private Map<String, ConnectionWrapperSet> topic2Subs = new ConcurrentHashMap<>();

    private ExecutorService exec = Executors.newCachedThreadPool();
    //schedule executor for backoff resume
    private final ScheduledExecutorService delayExec = Executors.newSingleThreadScheduledExecutor();

    private final float THRESDHOLD = 1.5f;
    private final float WATER_HIGH = 1.75f;
    private final float WATER_LOW = 0.001f;
    private final int INIT_DELAY = 20;
    private final int INTERVAL = 5;
    private final int INIT_RDY = 1;

    private AtomicBoolean start = new AtomicBoolean(false);
    private final Runnable REDISTRIBUTE_RUNNABLE = new Runnable() {
        @Override
        public void run() {
            redistributeRdy(ci.getLoadFactor(), ci.isConsumptionEstimateElapseTimeout(), ci.getRdyPerConnection());
        }
    };

    private final IConsumeInfo ci;
    public ConnectionManager(IConsumeInfo consumer) {
        this.ci = consumer;
    }

    /**
     *NSQConnectionWrapper, need it to override hashcode
     */
    public static class NSQConnectionWrapper extends Address {
        final private NSQConnection conn;

        public NSQConnectionWrapper(final NSQConnection conn) {
            super(conn.getAddress());
            this.conn = conn;
        }

        public NSQConnection getConn() {
            return conn;
        }
    }

    public static class ConnectionWrapperSet extends HashSet<NSQConnectionWrapper> {
        private AtomicInteger totalRdy = new AtomicInteger(0);
        private AtomicBoolean isBackOff = new AtomicBoolean(false);

        public void addTotalRdy(int rdy) {
            int rdyTotal = this.totalRdy.addAndGet(rdy);
            assert rdyTotal >= 0;
        }

         public int getTotalRdy() {
             return this.totalRdy.get();
         }

         public boolean backoff() {
            return isBackOff.compareAndSet(Boolean.FALSE, Boolean.TRUE);
         }

         public boolean resume() {
            return isBackOff.compareAndSet(Boolean.TRUE, Boolean.FALSE);
         }

         public boolean isBackoff() {
            return isBackOff.get();
         }

         public boolean remove(final Collection<Address> addrs) {
            return this.removeAll(addrs);
         }
    }

    public void start() {
        if(!start.compareAndSet(false, true))
            return;
        delayExec.scheduleWithFixedDelay(REDISTRIBUTE_RUNNABLE, INIT_DELAY, INTERVAL, TimeUnit.SECONDS);
    }

    public void start(int initDelay) {
        if(!start.compareAndSet(false, true))
            return;
        delayExec.scheduleWithFixedDelay(REDISTRIBUTE_RUNNABLE, initDelay, INTERVAL, TimeUnit.SECONDS);
    }

    boolean isStart() {
        return start.get();
    }

    //TODO: close
    public void close() {

    }

    /**
     * Not-thread safe, this method is invoked in a {@link com.youzan.nsq.client.Consumer}
     * @param topic     topic for subscribe
     * @param subscriber subscriber
     * @param rdy RDY to initialize connection
     */
    public void subscribe(String topic, final NSQConnection subscriber, int rdy) {
        if (null == subscriber || null == topic || topic.isEmpty() || !topic.equals(subscriber.getTopic().getTopicText())) {
            throw new IllegalArgumentException("topic: " + topic + " connection: " + subscriber);
        }

        if(!topic2Subs.containsKey(topic)) {
            topic2Subs.put(topic, new ConnectionWrapperSet());
        }

        final ConnectionWrapperSet subs = topic2Subs.get(topic);
        synchronized(subs) {
            subs.add(new NSQConnectionWrapper(subscriber));
            if(!subs.isBackoff()) {
                subscriber.onRdy(rdy, new IRdyCallback() {
                    @Override
                    public void onUpdated(int newRdy, int lastRdy) {
                        subs.addTotalRdy(subscriber.getCurrentRdyCount());
                    }
                });
            } else {
                backoff(subscriber);
            }
        }
    }

    public void subscribe(String topic, final NSQConnection subscriber) {
       subscribe(topic, subscriber, INIT_RDY);
    }

    /**
     * Not-thread safe, remove connection according to pass in addresses belong to topic
     * @param topic2Addrs topic to address collection map
     */
    public boolean remove(final Map<String, List<Address>> topic2Addrs) {
        boolean removed = false;
        for(String topic : topic2Addrs.keySet()) {
            if (topic2Subs.containsKey(topic)) {
                ConnectionWrapperSet subs = topic2Subs.get(topic);
                synchronized (subs) {
                    removed = removed | subs.remove(topic2Addrs.get(topic));
                    if(subs.size() == 0) {
                        topic2Subs.remove(topic);
                    }
                }
            }
        }
        return removed;
    }

    /**
     * backoff a single connection to nsqd
     * TODO: regardless of whether topic is backed off?
     * @param conn nsqd connection to backoff, connection manager check if connection belongs to current manager,
     *             backoff when it does.
     */
    public void backoff(final NSQConnection conn) {
        NSQConnectionWrapper connWrapper = new NSQConnectionWrapper(conn);
        String topic = conn.getTopic().getTopicText();

        if (!topic2Subs.containsKey(topic)) {
            logger.info("Subscriber for topic {} does not exist.");
            return;
        }

        final ConnectionWrapperSet conWrapperSet = topic2Subs.get(topic);
        if(conWrapperSet.contains(connWrapper)) {
            conn.onBackoff(new IRdyCallback() {
                @Override
                public void onUpdated(int newRdy, int lastRdy) {
                    conWrapperSet.addTotalRdy(newRdy - lastRdy);
                }
            });
        } else {
            logger.error("Connection {} does not belong to current consumer.", conn);
        }
    }

    /**
     * backoff connections to a topic.
     * @param topic topic to backoff.
     */
    public void backoff(final String topic) {
        if (!topic2Subs.containsKey(topic)) {
            logger.info("Subscriber for topic {} does not exist.");
            return;
        }

        //update lock
        final ConnectionWrapperSet subs = topic2Subs.get(topic);
        if(null != subs) {
            synchronized (subs) {
                if (!subs.backoff()) {
                    logger.info("topic {} is already backoff.");
                    return;
                }
                exec.submit(new Runnable() {
                    public void run() {
                        for (NSQConnectionWrapper sub : subs) {
                            try {
                                sub.getConn().onBackoff(new IRdyCallback() {
                                    @Override
                                    public void onUpdated(int newRdy, int lastRdy) {
                                        subs.addTotalRdy(newRdy - lastRdy);
                                    }
                                });
                            } catch (Exception e) {
                                logger.error("Error on backing off connection {}", sub);
                            }
                        }
                        logger.info("Backoff connection(s) notify send for topic {}", topic);
                    }
                });
            }
        }
    }


    public void resume(final String topic) {
        if (!topic2Subs.containsKey(topic)) {
            logger.info("Subscriber for topic {} does not exist.");
            return;
        }

        final ConnectionWrapperSet subs = topic2Subs.get(topic);
        if(null != subs) {
            synchronized (subs) {
                if(!subs.resume()) {
                    logger.info("topic {} is already in resumed.");
                    return;
                }
                exec.submit(new Runnable() {
                    @Override
                    public void run() {
                        for (NSQConnectionWrapper sub : subs) {
                            sub.getConn().onResume(new IRdyCallback() {
                                @Override
                                public void onUpdated(int newRdy, int lastRdy) {
                                    subs.addTotalRdy(newRdy - lastRdy);
                                }
                            });
                        }
                        logger.info("Resume connection(s) notify send for topic {}", topic);
                    }
                });
            }
        }
    }

    private void redistributeRdy(float scheduleLoad, boolean mayTimeout, int rdyPerCon) {
       for(String topic:topic2Subs.keySet()) {
           final ConnectionWrapperSet subs = topic2Subs.get(topic);
           if(null != subs) {
               synchronized (subs) {
                   if (!mayTimeout && scheduleLoad <= THRESDHOLD && scheduleLoad > WATER_LOW && !subs.isBackoff()) {
                       for (NSQConnectionWrapper sub : subs) {
                           final NSQConnection con = sub.getConn();
                           int currentRdy = con.getCurrentRdyCount();
                           final int availableRdy = rdyPerCon * subs.size() - subs.getTotalRdy() + currentRdy;
                           final int expectedRdy = con.getExpectedRdy();
                           if (availableRdy > 0) {
                               int ceilingRdy = availableRdy > expectedRdy ? expectedRdy : availableRdy;
//                                  TODO now we do not exceed expected per connection
//                                  if(currentRdy >= expectedRdy && availableRdy > ceilingRdy)
//                                  ceilingRdy = availableRdy;
                               final int newRdy = Math.min(ceilingRdy, currentRdy + 1);
                               if (newRdy > currentRdy) {
                                   con.command(new Rdy(newRdy)).addListener(new ChannelFutureListener() {
                                       @Override
                                       public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                           if (channelFuture.isSuccess()) {
                                               int lastRdy = con.getCurrentRdyCount();
                                               con.setCurrentRdyCount(newRdy);
                                               subs.addTotalRdy(newRdy - lastRdy);
                                           }
                                       }
                                   });
                               }
                           }
                       }
                   } else if (((scheduleLoad >= WATER_HIGH && mayTimeout)|| scheduleLoad <= WATER_LOW) && !subs.isBackoff()) {
                       //rdy decrease
                       for (NSQConnectionWrapper sub : subs) {
                           final NSQConnection con = sub.getConn();
                           int currentRdy = con.getCurrentRdyCount();
                           if (currentRdy > 1) {
                               //update rdy
                               final int expectedRdy = con.getExpectedRdy();
                               final int newRdy = Math.min(currentRdy - 1, expectedRdy);
                               con.command(new Rdy(newRdy)).addListener(new ChannelFutureListener() {
                                   @Override
                                   public void operationComplete(ChannelFuture channelFuture) throws Exception {
                                       if (channelFuture.isSuccess()) {
                                           int lastRdy = con.getCurrentRdyCount();
                                           con.setCurrentRdyCount(newRdy);
                                           subs.addTotalRdy(newRdy - lastRdy);
                                       }
                                   }
                               });
                           }
                       }
                   }
               }
           }
       }
    }

    public Set<NSQConnectionWrapper> getSubscribeConnections(String topic) {
        return this.topic2Subs.get(topic);
    }
}
