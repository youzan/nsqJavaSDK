package com.youzan.nsq.client;

/**
 * Created by lin on 16/10/27.
 */
public class PropertyNotFoundException extends RuntimeException{

    private String property;

    public PropertyNotFoundException(String property, String msg){
        super(msg);
        this.property = property;
    }

}
