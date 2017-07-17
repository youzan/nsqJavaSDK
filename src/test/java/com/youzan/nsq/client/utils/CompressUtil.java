package com.youzan.nsq.client.utils;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 压缩解压工具
 * Created by suguoqing on 2017/6/26.
 */
public class CompressUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompressUtil.class);

    /**
     * 对象json 序列化, lz4 压缩
     * 返回数据的前4位存 原始数据的长度
     *
     * @param object
     * @return
     */
    public static byte[] compress(Object object) {
        byte[] sourceBytes = JSON.toJSONBytes(object);

        byte[] compressed = Lz4Util.compress(sourceBytes);

        byte[] finalBytes = new byte[compressed.length + 4];

        LOGGER.info("sourceLen ={}, compressLen={}", sourceBytes.length, finalBytes.length);

        BytesUtil.writeInt(finalBytes, 0, sourceBytes.length);
        System.arraycopy(compressed, 0, finalBytes, 4, compressed.length);

        return finalBytes;
    }

    public static <T> T decompress(byte[] compressBytes, Class<T> dataType) {
        int length = BytesUtil.readInt(compressBytes, 0);

        if (length <= 0) {
            return null;
        }

        byte[] compressData = Arrays.copyOfRange(compressBytes, 4, compressBytes.length);

        LOGGER.info("sourceLen ={}, compressLen={}", length, compressBytes.length);

        if(length > 5000000){
            LOGGER.warn("Length >5m, sourceLen ={}, compressLen={}", length, compressBytes.length);
            return null;
        }

        byte[] decompressedData = Lz4Util.decompress(compressData, length);

        return JSON.parseObject(decompressedData, dataType);
    }
}
