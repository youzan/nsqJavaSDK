package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Role;

/**
 * Migration config access key for DCC.
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessKey extends AbstractConfigAccessKey<Role> {

    private DCCMigrationConfigAccessKey(Role key) {
        super(key);
    }

    @Override
    public String toKey() {
        return this.innerKey.getRoleTxt();
    }

    public static AbstractConfigAccessKey getInstance(Role role) {
        return new DCCMigrationConfigAccessKey(role);
    }
}
