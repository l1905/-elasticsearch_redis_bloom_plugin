package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class HistoryBloomFilterHandle {

    private static final Logger logger = LogManager.getLogger(HistoryBloomFilterHandle.class.getName());

    /**
     * 主库
     */
    private JedisPool jedisPool;

    /**
     * 取最近的30个bloomfilter块
     */
    private long defaultBloomfilterNum = 30;
    /**
     * 保留bloom块的个数， 理论上可以多冗余一段时间
     */
    private long keepBloomfilterNum = 30;
    /**
     * 每个bloomFilter块能存储的文章数4000
     */
    private long maxElementCount = 4000;
    /**
     * 误差率千分之一
     */
    private static final double FALSE_POSITIVES = 0.001;
    /**
     * bloom块过期时间为18天, 18 * 8640
     */
    private int expireTime = 18 * 8640;
    /**
     * 取最近的30个bloomfilter块
     */
    private final List<BloomFilter<byte[]>> bloomFilterBlockList = new ArrayList<>();

    private Set<String> devices = new HashSet<String>();
    /**
     * 构造器，初始化依赖资源
     */
    public HistoryBloomFilterHandle(JedisPool jedisBloomfilterHistoryPool) {
        // 初始化redis实例
        jedisPool = jedisBloomfilterHistoryPool;

    }

    public boolean checkHistoryShow(String item) {
        for (BloomFilter<byte[]> bloomFilterBlock : bloomFilterBlockList) {
            if (bloomFilterBlock.mightContain(item.getBytes())) {
                return true;
            }
        }
        return false;
    }

    // 拼接bloom中字符串格式
//    private String getElementKey(ItemDataEntity item) {
//        String element = "";
//        if(item.getIsHomeAfresh() == null || item.getIsHomeAfresh() ==0) {
//            element =  item.getChannel()+"-"+item.getItemId() ;
//        } else {
//            element =  item.getChannel()+"-"+item.getItemId()+"-"+ item.getIsHomeAfresh();
//        }
//        return element;
//    }

    public void readBloomFilter(String userId, String deviceId) {

        String metaKey = getMetaKey(userId, deviceId);
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> metaList;
            metaList = jedis.lrange(metaKey, 0, defaultBloomfilterNum);
            byte[] rawData = null;
            // 首个块读取主库
            if (metaList.size() > 0) {
                rawData = jedis.get(metaList.get(0).getBytes());
                if (rawData != null) {
                    ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(rawData);
                    BloomFilter<byte[]> bloomFilterBlock = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                    bloomFilterBlockList.add(bloomFilterBlock);
                }
            }
            // 剩余块,pipeline读取从库
            List<Response<byte[]>> rawPipeDataList = new ArrayList<>();
            Pipeline jedisSlavePipe = jedis.pipelined();
            for (int i = 1; i < metaList.size(); i++) {
                String blockKey = metaList.get(i);
                Response<byte[]> rawPipeData = jedisSlavePipe.get(blockKey.getBytes());
                rawPipeDataList.add(rawPipeData);
            }
            jedisSlavePipe.sync();
            for (Response<byte[]> resp : rawPipeDataList) {
                rawData = resp.get();
                if (rawData != null) {
                    ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(rawData);
                    BloomFilter<byte[]> bloomFilterBlock = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                    bloomFilterBlockList.add(bloomFilterBlock);
                }
            }
        } catch (Exception e) {
            logger.error("readBloomfilter error msg is ", e);
        }
    }

    public boolean writeBloomFilter(String userId, String deviceId, List<String> itemList) {
        boolean status = true;
        try (Jedis jedis = jedisPool.getResource()) {
            // meta 元信息key
            String metaKey = getMetaKey(userId, deviceId);
            String blockKey = "";
            List<String> metaList = jedis.lrange(metaKey, 0, 1);
            byte[] rawData = null;
            if (metaList.size() > 0) {
                blockKey = metaList.get(0);
                rawData = jedis.get(blockKey.getBytes());
            }
            String statusTxt = "add";
            BloomFilter<byte[]> bloomFilterBlock;
            if (rawData != null) {
                ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(rawData);
                bloomFilterBlock = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                // 数据块放入不足
                if (maxElementCount - bloomFilterBlock.approximateElementCount() >= itemList.size()) {
                    // 更新
                    statusTxt = "update";
                } else {
                    // 新增bloom块
                    bloomFilterBlock = BloomFilter.create(Funnels.byteArrayFunnel(), maxElementCount, FALSE_POSITIVES);
                }
            } else {
                // 新增bloom块
                bloomFilterBlock = BloomFilter.create(Funnels.byteArrayFunnel(), maxElementCount, FALSE_POSITIVES);
            }
//            + 1 操作
            for (String item : itemList) {
                bloomFilterBlock.put(item.getBytes());
            }
            try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();) {
                bloomFilterBlock.writeTo(byteArrayOutputStream);
                if ("update".equals(statusTxt)) {
                    jedis.setex(blockKey.getBytes(), expireTime, byteArrayOutputStream.toByteArray());
                } else {
                    long timestamp = System.currentTimeMillis() * 1000000L + System.nanoTime() % 1000000L;
                    blockKey = getBlockKey(userId, deviceId, timestamp);

                    Pipeline jedisPipe = jedis.pipelined();
                    jedisPipe.lpush(metaKey, blockKey);
                    jedisPipe.ltrim(metaKey, 0, keepBloomfilterNum - 1);
                    jedisPipe.expire(metaKey, expireTime); // 设置key过期时间
                    jedisPipe.setex(blockKey.getBytes(), expireTime, byteArrayOutputStream.toByteArray());
                    jedisPipe.sync();
                }
            }
            // 写入成功， 判断用户ID是否在白名单中， 则加入明文列表 TODO
            if(devices.contains(deviceId)) {
                Pipeline jedisPipe2 = jedis.pipelined();
                String whitekey = getWhiteKey(deviceId);
                jedisPipe2.lpush(whitekey, itemList.toArray(new String[itemList.size()]));
                jedisPipe2.ltrim(whitekey, 0, 10000);
                jedisPipe2.expire(whitekey, expireTime);
                jedisPipe2.sync();
            }
        } catch (Exception e) {
            status = false;
            logger.error("writeBloomfilter error msg is ", e);
        }
        return status;
    }

    // 测试使用
    public boolean deleteBloomFilter(String userId, String deviceId) {
        boolean status = true;
        String metaKey = getMetaKey(userId, deviceId);
        try (Jedis jedis = jedisPool.getResource()) {
            // 这里只删除meta_key
            jedis.del(metaKey);
        } catch (Exception e) {
            status = false;
            logger.error("readBloomfilter error msg is {}", e.getMessage());
        }
        return status;
    }

    private String getMetaKey(String userId, String deviceId) {
        String metaKey = "b_meta" +"_"+ userId + "_" + deviceId;
        return metaKey;
    }

    private String getBlockKey(String userId, String deviceId, long timestamp) {
        String blockKey = "b_block" + "_" + userId + "_" + deviceId + "_" + timestamp;
        return blockKey;
    }

    private String getWhiteKey(String deviceId) {
        String whitekey = "bloom_white_"+ deviceId;
        return whitekey;
    }

    public List<String> getWhiteList(String deviceId) {
        List<String> whiteList = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            String whiteKey = getWhiteKey(deviceId);

            whiteList = jedis.lrange(whiteKey, 0, -1);
        } catch (Exception e) {
            logger.error("getWhiteList error msg is ", e);
        }
        return whiteList;
    }





}

