package com.youzan.nsq.client;

import java.util.Set;

/**
 * Interface mainly works on returning partition Id for {@link Consumer} to help decide which partition to connect to
 * in SUB ORDER situation. By implementing interfaces to fit your purpose for SUB ORDER.
 * Created by lin on 16/12/16.
 */
public interface IPartitionSelector {
    int pickPartition();
    Set getPartitionIDs();
}
