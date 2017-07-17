package com.youzan.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

public final class IOUtil {

    private static final Logger logger = LoggerFactory.getLogger(IOUtil.class);

    public static final Charset ASCII = StandardCharsets.US_ASCII;
    public static final Charset UTF8 = StandardCharsets.UTF_8;
    public static final Charset DEFAULT_CHARSET = UTF8;
    private volatile static int HTTP_IO_CONN_TIMEOUT = 5 * 1000;
    private volatile static int HTTP_IO_READ_TIMEOUT = 10 * 1000;

    public static void setHTTPConnectionTimeout(int newTimeout) {
        HTTP_IO_CONN_TIMEOUT = newTimeout;
    }

    public static void setHTTPReadTimeout(int newTimeout) {
        HTTP_IO_READ_TIMEOUT = newTimeout;
    }

    public static void closeQuietly(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (null != closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    logger.error("Exception:", e);
                }
            }
        }
    }

    public static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (null != closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    logger.error("Exception:", e);
                }
            }
        }
    }

    /**
     * request http GET for pass in URL, then parse response to json, some predefined
     * header properties are added here, like Accept: application/vnd.nsq;
     * stream as json
     *
     * @param url url of json resource
     * @return jsonNode
     * @throws IOException {@link IOException}
     */
    public static JsonNode readFromUrl(final URL url) throws IOException {
        logger.debug("Prepare to open HTTP Connection...");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setConnectTimeout(HTTP_IO_CONN_TIMEOUT);
        con.setReadTimeout(HTTP_IO_READ_TIMEOUT);
        //skip that, as GET is default operation
        //add request header, to support nsq of new version
        con.setRequestProperty("Accept", "application/vnd.nsq; version=1.0");
        if (logger.isDebugEnabled()) {
            logger.debug("Request to {} responses {}:{}.", url.toString(), con.getResponseCode(), con.getResponseMessage());
        }
        //jackson handles InputStream close operation
        return SystemUtil.getObjectMapper().readTree(con.getInputStream());
    }

    public static void requestToUrl(String method, final URL url, String content) throws Exception {
        logger.info("{}: {}. Content {}", method, url.toString(), content);
        URLConnection connection = url.openConnection();
        HttpURLConnection httpURLConnection = (HttpURLConnection)connection;

        if(null != content)
            httpURLConnection.setDoOutput(true);
        else
            httpURLConnection.setDoOutput(false);

        httpURLConnection.setDoInput(true);
        httpURLConnection.setRequestMethod(method);
        httpURLConnection.setRequestProperty("Accept-Charset", "utf-8");

        OutputStream outputStream = null;
        OutputStreamWriter outputStreamWriter = null;
        httpURLConnection.connect();
        if (null != content) {
            try {
                outputStream = httpURLConnection.getOutputStream();
                outputStreamWriter = new OutputStreamWriter(outputStream);

                outputStreamWriter.write(content);
                outputStreamWriter.flush();
            } finally {
                if (outputStreamWriter != null) {
                    outputStreamWriter.close();
                }

                if (outputStream != null) {
                    outputStream.close();
                }
            }
        }
        if (httpURLConnection.getResponseCode() >= 300) {
            throw new Exception("HTTP Request failed, Response code is " + httpURLConnection.getResponseCode());
        }
        httpURLConnection.disconnect();
        Thread.sleep(1000);
    }

    public static void postToUrl(final URL url, String content) throws Exception {
       requestToUrl("POST", url, content);
    }

    public static void deleteToUrl(final URL url) throws Exception {
        requestToUrl("DELETE", url, null);
    }

    public static byte[] compress(String str) throws IOException {
        if (str == null || str.length() == 0) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes());
        gzip.close();
        return out.toByteArray();
    }

}
