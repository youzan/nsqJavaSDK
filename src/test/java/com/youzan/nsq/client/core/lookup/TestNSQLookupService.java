package com.youzan.nsq.client.core.lookup;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TestNSQLookupService {
    private static final Logger logger = LoggerFactory.getLogger(TestNSQLookupService.class);

    @DataProvider
    public Object[][] generateIPs() {
        Object[][] objs = new Object[2][2];

        String ips = "10.232.120.12:6411";
        List<String> expected = new ArrayList<>(10);
        expected.add("10.232.120.12:6411");
        objs[0][0] = ips;
        objs[0][1] = expected;

        ips = "10.232.120.13 : 6411";
        expected = new ArrayList<>(10);
        expected.add("10.232.120.13:6411");
        objs[1][0] = ips;
        objs[1][1] = expected;

        return objs;
    }

    @Test
    public void simpleInit() {
        try (LookupServiceImpl srv = new LookupServiceImpl("10.232.120.12:6411")) {
            for (String addr : srv.getAddresses()) {
                Assert.assertTrue(addr.split(":").length == 2);
                Assert.assertEquals(addr, "10.232.120.12:6411");
            }
        }
    }

    private OutputStreamAppender addByteArrayOutputStreamAppender(Logger log){
        // Destination stream
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        // Get LoggerContext from SLF4J
        LoggerContext context = (LoggerContext)LoggerFactory.getILoggerFactory();

        // Encoder
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%d{HH:mm:ss} %-5level %logger{36} - %msg%n");
        encoder.start();

        // OutputStreamAppender
        OutputStreamAppender<ILoggingEvent> appender= new OutputStreamAppender<>();
        appender.setName( "OutputStream Appender" );
        appender.setContext(context);
        appender.setEncoder(encoder);
        appender.setOutputStream(stream);

        appender.start();

        ((ch.qos.logback.classic.Logger)log).addAppender(appender);
        return appender;
    }

    @Test
    public void makeBadOfLookup4ConnectionTimeoutTrace() throws NoSuchMethodException, NoSuchFieldException, IllegalAccessException, InvocationTargetException, InstantiationException, UnsupportedEncodingException {
        //create LookupServiceImpl vis reflect, and inject appender into logger
        Class lookupClazz = LookupServiceImpl.class;
        Constructor constructor = lookupClazz.getConstructor(String.class);
        LookupServiceImpl lsi = null;
        lsi = (LookupServiceImpl) constructor.newInstance("127.0.0.1:2333");

        //fetch the logger, which is a static private
        Field logFld = lookupClazz.getDeclaredField("logger");
        logFld.setAccessible(true);
        Logger log = (Logger) logFld.get(lsi);

        //add appender to redirect log output
        OutputStreamAppender appender = addByteArrayOutputStreamAppender(log);
        try{
            lsi.start();
            //main thread sleeps for 90secs in order to give lookup service
            //enough time to run.
            Thread.sleep(90000l);
        }catch (InterruptedException e) {
            e.printStackTrace();
        }
        ByteArrayOutputStream baos = (ByteArrayOutputStream) appender.getOutputStream();
        String logOutput = baos.toString("utf-8");
        Assert.assertTrue(logOutput.contains("Fail to connect to NSQ lookup. client, "));
    }

    @Test(dataProvider = "genIPs")
    public void testInit(String ips, List<String> expected) {
        try (LookupServiceImpl srv = new LookupServiceImpl(ips)) {
            Assert.assertEquals(srv.getAddresses(), expected);
        }
    }
}
