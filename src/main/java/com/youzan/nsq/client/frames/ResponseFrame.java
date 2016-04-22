package com.youzan.nsq.client.frames;

import com.youzan.nsq.client.enums.ResponseType;

/**
 * @author caohaihong
 * since 2015年10月29日 下午2:51:23
 */
public class ResponseFrame implements NSQFrame {
    private String response;
    private ResponseType responseType;
    
    public ResponseFrame(ResponseType resp) {
        this.responseType = resp;
    }
    
    public ResponseFrame(String resp) {
        this.response = resp;
    }
    
    public ResponseType getResponseType() {
        return responseType;
    }
    
    public String getResponse() {
        return response;
    }

    @Override
    public String getMessage() {
        if (responseType == null) return response;
        return responseType.getCode();
    }
}
