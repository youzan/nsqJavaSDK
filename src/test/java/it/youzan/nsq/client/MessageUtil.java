package it.youzan.nsq.client;

import java.util.Date;

public final class MessageUtil {

    public static String randomString() {
        return "The message is " + new Date().getTime() + " .";
    }
}
