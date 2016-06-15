/**
 * 
 */
package com.youzan.nsq.client.entity;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public enum Response {

    OK("OK"), //
    _HEARTBEAT_("_heartbeat_"),//
    // Error Responses
    E_INVALID("E_INVALID"),//
    E_BAD_TOPIC("E_BAD_TOPIC"), //
    E_TOPIC_NOT_EXIST("E_TOPIC_NOT_EXIST"),//
    E_BAD_MESSAGE("E_BAD_MESSAGE"), //
    E_PUB_FAILED("E_PUB_FAILED"), //
    E_BAD_BODY("E_BAD_BODY"), //
    E_MPUB_FAILED("E_MPUB_FAILED"),//
    E_FAILED_ON_NOT_LEADER("E_FAILED_ON_NOT_LEADER"),//
    E_FAILED_ON_NOT_WRITABLE("E_FAILED_ON_NOT_WRITABLE"), //
    ;

    private final String content;

    Response(String content) {
        this.content = content;
    }

    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

}
