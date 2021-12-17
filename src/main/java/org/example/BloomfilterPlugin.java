package org.example;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.*;
import org.elasticsearch.search.lookup.SearchLookup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An example script plugin that adds a {@link ScriptEngine}
 * implementing expert scoring.
 */
public class BloomfilterPlugin extends Plugin implements ScriptPlugin {

    private static final Logger logger = LogManager.getLogger(BloomfilterPlugin.class.getName());

    @Override
    public ScriptEngine getScriptEngine(
            Settings settings,
            Collection<ScriptContext<?>> contexts
    ) {
        return new BloomFilterScriptEngine();
    }

    /**
     * An example {@link ScriptEngine} that uses Lucene segment details to
     * implement pure document frequency scoring.
     */
    // tag::expert_engine
    private static class BloomFilterScriptEngine implements ScriptEngine {
        // 通过这两个字段判断是否使用该插件
        private final String _SOURCE_VALUE = "UserReadBloomFilter";
        private final String _LANG_VALUE = "BloomFilter";

        @Override
        public String getType() {
            return _LANG_VALUE;
        }

        @Override
        public <T> T compile(
                String scriptName,
                String scriptSource,
                ScriptContext<T> context,
                Map<String, String> params
        ) {
            if (context.equals(ScoreScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }
            // we use the script "source" as the script identifier
            if (_SOURCE_VALUE.equals(scriptSource)) {
                ScoreScript.Factory factory = new UserBloomFilterFactory();
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("Unknown script name "
                    + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(ScoreScript.CONTEXT);
        }

        private static class UserBloomFilterFactory implements ScoreScript.Factory,
                ScriptFactory {
            private JedisPool jedisPool;
            LoadingCache<String, HistoryBloomFilterHandle> cache;

            // 可以设置本地缓存， 大小atomic方式

            @Override
            public boolean isResultDeterministic() {
                // PureDfLeafFactory only uses deterministic APIs, this
                // implies the results are cacheable.
                return true;
            }

            private UserBloomFilterFactory() {
                // 初始化Redis资源
                String filename = "config.properties";
                logger.info("加载配置文件 {}", filename);
                InputStream inputStream = BloomfilterPlugin.class.getClassLoader().getResourceAsStream(filename);
                Properties properties = new Properties();
                try {
                    properties.load(inputStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxIdle(Integer.parseInt(properties.getProperty("jedis.bloomfilter.maxIdle")));
                jedisPoolConfig.setMaxTotal(Integer.parseInt(properties.getProperty("jedis.bloomfilter.maxTotal")));
                jedisPoolConfig.setMaxWaitMillis(Integer.parseInt(properties.getProperty("jedis.bloomfilter.maxWaitMillis")));
                jedisPoolConfig.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("jedis.bloomfilter.testOnBorrow")));
                // 避免使用
                jedisPoolConfig.setJmxEnabled(false);
                logger.warn("jedis.bloomfilter.maxIdle: {}", properties.getProperty("jedis.bloomfilter.maxIdle"));

                // 加载redis连接池
                // 需要引入本地缓存，多路找回，会重复查询该用户的已读bloom块，或已读历史
                jedisPool = AccessController.doPrivileged((PrivilegedAction<JedisPool>) () -> {
                    return new JedisPool(jedisPoolConfig, properties.getProperty("jedis.bloomfilter.host"),
                            Integer.parseInt(properties.getProperty("jedis.bloomfilter.port")), 10000, properties.getProperty("jedis.bloomfilter.password"));
                });
                cache = AccessController.doPrivileged((PrivilegedAction<LoadingCache<String, HistoryBloomFilterHandle>>) () -> {
                    return CacheBuilder.newBuilder()
                            // 设置缓存在写入3秒后，通过CacheLoader的load方法进行刷新
                            .refreshAfterWrite(5, TimeUnit.SECONDS)
                            .initialCapacity(100)
                            .maximumSize(2000)
                            .removalListener(notification -> {
                                System.out.println(notification.getKey() + " " + notification.getValue() + " 被移除,原因:" + notification.getCause());
                            })
                            // jdk8以后可以使用 Duration
                            // .refreshAfterWrite(Duration.ofMinutes(10))
                            .build(new CacheLoader<String, HistoryBloomFilterHandle>() {
                                @Override
                                public HistoryBloomFilterHandle load(String key) throws Exception {
                                    HistoryBloomFilterHandle handle = new HistoryBloomFilterHandle(jedisPool);
                                    String[] item = key.split("-");
                                    String userId = item[0];
                                    String deviceId = item[1];
                                    // 测试逻辑
                                    List<String> itemList = new ArrayList<>();
                                    itemList.add("96");
                                    itemList.add("91");
                                    itemList.add("86");
                                    handle.writeBloomFilter(userId, deviceId, itemList);

                                    handle.readBloomFilter(userId, deviceId);

                                    // 测试逻辑
                                    try (Jedis jedis = jedisPool.getResource()) {
                                        long count = jedis.incr("test");
                                        logger.warn("redis_count2: {}", count);
                                    } catch (Exception e) {
                                        logger.error("jedisPool.getResource Exception: {}", e.getMessage());
                                    }
                                    return handle;
                                }
                            });

                });

            }

            @Override
            public ScoreScript.LeafFactory newFactory(
                    Map<String, Object> params,
                    SearchLookup lookup
            ) {
                return new UserBloomFilterLeafFactory(params, lookup, jedisPool, cache);
            }
        }

        private static class UserBloomFilterLeafFactory implements ScoreScript.LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            private final String field;
            private final String userId;
            private final String deviceId;
            private JedisPool jedisBloomPool;
            private HistoryBloomFilterHandle historyBloomFilterHandle;

            private UserBloomFilterLeafFactory(
                    Map<String, Object> params, SearchLookup lookup, JedisPool jedisBloomPool, LoadingCache<String, HistoryBloomFilterHandle> cache) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException(
                            "Missing parameter [field]");
                }
                if (params.containsKey("user_id") == false) {
                    throw new IllegalArgumentException(
                            "Missing parameter [user_id]");
                }
                if (params.containsKey("device_id") == false) {
                    throw new IllegalArgumentException(
                            "Missing parameter [device_id]");
                }

                this.params = params;
                this.lookup = lookup;
                field = params.get("field").toString();
                userId = params.get("user_id").toString();
                deviceId = params.get("device_id").toString();

                // 这里应该需要拿Bloom块
                this.jedisBloomPool = jedisBloomPool;

                // 必须要在doPrivileged中判断是否存在， 否则会报Could not get a resource from the pool
                this.historyBloomFilterHandle = AccessController.doPrivileged((PrivilegedAction<HistoryBloomFilterHandle>) () -> {
                    try {
                        return (HistoryBloomFilterHandle) cache.get(userId+"-"+deviceId);
                    } catch (ExecutionException e) {
                        logger.warn("获取historyBloomfilterHandle失败, msg:{}", e.getMessage());
                        return null;
                    }
                });

                logger.info("field:{}, user_id:{}", field, userId);
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public ScoreScript newInstance(DocReader docReader)
                    throws IOException {
                // 构造器初始化
                return new ScoreScript(params, lookup, docReader) {

                    // 获取docid
                    public void setDocument(int docid) {
                        logger.warn("setDocument:docid:{}", docid);

                        super.setDocument(docid);
                    }

                    @Override
                    public double execute(ExplanationHolder explanationHolder) {
                        String score = this.getDoc().get("score").get(0).toString();

                        // 需要判断用户对该便文章是否已读, 如果为0， 则讲分数设置为0， 其他设置为1
                        if(historyBloomFilterHandle != null && historyBloomFilterHandle.checkHistoryShow(score)) {
                            score = "0";
                        }

                        // 获取分数
                        return Double.parseDouble(score);

                    }
                };
            }
        }
        // end::expert_engine
    }
}
