package com.youzan.nsq.client.configs;

import com.youzan.nsq.client.entity.Role;

/**
 * Created by lin on 16/12/9.
 */
public class DCCMigrationConfigAccessKey extends AbstractConfigAccessKey<Role> {

    public DCCMigrationConfigAccessKey(Role key) {
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
