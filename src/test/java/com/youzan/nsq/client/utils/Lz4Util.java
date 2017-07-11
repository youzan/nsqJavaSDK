package com.youzan.nsq.client.utils; /**
 * Created by lin on 17/6/30.
 */

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by suguoqing on 16/6/8.
 */
public class Lz4Util {
    private static final Logger LOGGER = LoggerFactory.getLogger(Lz4Util.class);
    private static LZ4Factory factory = LZ4Factory.fastestInstance();
    private static LZ4SafeDecompressor decompressor = factory.safeDecompressor();
    private static LZ4FastDecompressor fastDecompressor = factory.fastDecompressor();
    private static LZ4Compressor compressor = factory.fastCompressor();

    /**
     * 压缩一个 data 数组
     * @param data 原始数据
     * @return 压缩后的数据
     */
    public static byte[] compress(byte[] data){
        final int decompressedLength = data.length;

        // compress data
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);

        byte[] result = new byte[compressedLength];
        System.arraycopy(compressed, 0, result, 0, compressedLength);
        return result;
    }

    /**
     * 快速解压一个数组
     * @param compressedArray  压缩后的数据
     * @param orinArrayLength
     * @param extInfo
     * @return
     */
    public static byte[] fastDecopress(final byte[] compressedArray, int orinArrayLength, String... extInfo) {
        try {
            return fastDecompressor.decompress(compressedArray, orinArrayLength);
        } catch (Exception e) {
            LOGGER.error("fast decompress Error:extInfo ={} ", extInfo, e);
            return null;
        }
    }

    /**
     * 解压一个数组
     * @param finalCompressedArray  原始数组
     * @param decompressLength      数组压缩前的长度
     * @param extInfo               异常信息
     * @return
     */
    public static byte[] decompress( byte[] finalCompressedArray, Integer decompressLength, String ... extInfo) {
        int ratio = 3;
        if (null != decompressLength) {
            // 知道长度用fastCompress
            return fastDecopress(finalCompressedArray, decompressLength, extInfo);
        } else {
            decompressLength = finalCompressedArray.length * ratio;
        }

        int i = 5;
        while (i > 0) {
            try {
                return decompress(finalCompressedArray, decompressLength);
            } catch (Exception e) {
                ratio = ratio * 2;
                i--;
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("decompress Error:ratio ={} extInfo ={} ", ratio, extInfo, e);
                }

            }

        }

        throw new RuntimeException("decompress error");
    }

    /**
     * 解压一个数组
     *
     * @param finalCompressedArray 压缩后的数据
     * @param length               原始数据长度, 长度不能小于压缩前的长度。
     * @return
     */
    private static byte[] decompress(byte[] finalCompressedArray, int length) {

        byte[] desc = new byte[length];
        int decompressLen = decompressor.decompress(finalCompressedArray, desc);

        byte[] result = new byte[decompressLen];
        System.arraycopy(desc, 0, result, 0, decompressLen);
        return result;
    }

}