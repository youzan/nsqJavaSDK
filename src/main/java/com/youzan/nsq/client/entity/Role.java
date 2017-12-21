package com.youzan.nsq.client.entity;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 */
public enum Role {
    Consumer("consumer"),
    Producer("producer"),;

    private String roleTxt;

    Role(String roleTxt) {
        this.roleTxt = roleTxt;
    }

    public String getRoleTxt(){
        return this.roleTxt;
    }
}
