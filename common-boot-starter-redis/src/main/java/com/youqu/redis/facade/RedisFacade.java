package com.youqu.redis.facade;

import com.youqu.redis.properties.RedisConfigProperties;
import com.youqu.redis.routes.RedisTemplateRoute;
import com.youqu.redis.utils.ObjectUtil;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 操作redis
 * Created by wangpeng on 2018/4/24.
 */
public class RedisFacade {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private static Logger cacheLogger = LoggerFactory.getLogger("redisCachePerf4j");

    private final static String EMPTY_VALUE = "null";

    private RedisConfigProperties redisConfigProperties;

    private RedisTemplateRoute redisTemplateRoute;

    private RedissonClient redissonClient;

    public RedisFacade(RedisConfigProperties redisConfigProperties, RedisTemplateRoute redisTemplateRoute, RedissonClient redissonClient) {
        this.redisConfigProperties = redisConfigProperties;
        this.redisTemplateRoute = redisTemplateRoute;
        this.redissonClient = redissonClient;
    }

    /**
     * 缓存存入空值
     *
     * @param key
     * @param time
     * @param timeUnit
     */
    public void writeEmpty(String key, long time, TimeUnit timeUnit) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        logger.debug("返回值为null，缓存空值,key={},time={},timeUnit={}", key, time, timeUnit);
        redisTemplateRoute.getTemplate(key).boundValueOps(key).set("", time, timeUnit);
        logger.debug("空值写入成功,key={}", key);
        stopWatch.stop("redis.writeEmpty");
    }

    /**
     * 写入缓存
     *
     * @param key
     * @param value
     * @param time
     * @param timeUnit
     */
    public void writeString(String key, String value, long time, TimeUnit timeUnit) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        logger.debug("写入缓存,key={},value={},time={},timeUnit={}", key, value, time, timeUnit);
        redisTemplateRoute.getTemplate(key).boundValueOps(key).set(value, time, timeUnit);
        logger.debug("缓存写入成功,key={}", key);
        stopWatch.stop("redis.writeCache");
    }

    /**
     * 写入不带过期时间的key
     *
     * @param key
     * @param value
     */
    public void writeString(String key, String value) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        logger.debug("写入缓存,key={},value={}", key, value);
        redisTemplateRoute.getTemplate(key).boundValueOps(key).set(value);
        logger.debug("缓存写入成功,key={}", key);
        stopWatch.stop("redis.writeCache");
    }

    /**
     * 获取缓存值
     *
     * @return
     */
    public String getValue(String key) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Object value = redisTemplateRoute.getTemplate(key).boundValueOps(key).get();
        stopWatch.stop("redis.getValue");
        if (value != null) {
            return value.toString();
        } else {
            return null;
        }
    }

    /**
     * 设置key 过期时间
     *
     * @param key
     * @param timeOut
     * @param timeUnit
     * @return
     */
    public Boolean setExpireTime(String key, long timeOut, TimeUnit timeUnit) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Boolean result = redisTemplateRoute.getTemplate(key).expire(key, timeOut, timeUnit);
        stopWatch.stop("redis.setExpireTime");
        return result;
    }

    /**
     * 自增并返回自增后的值
     *
     * @param key
     * @param delta
     * @return
     */
    public Long incAndGet(String key, long delta) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Long increment = redisTemplateRoute.getTemplate(key).boundValueOps(key).increment(delta);
        stopWatch.stop("redis.incAndGet");
        return increment;
    }

    /**
     * 存入hash
     *
     * @param key
     * @param value
     * @param <T>
     */
    public <T> void writeHash(String key, T value, int timeOut, TimeUnit timeUnit) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Map<String, ?> mappedHash = null;
        try {
            mappedHash = ObjectUtil.convertBean(value);
            stopWatch.lap("redis.writeHash.convertHashToMap");
        } catch (Exception e) {
            e.printStackTrace();
        }
        redisTemplateRoute.getTemplate(key).boundHashOps(key).putAll(mappedHash);
        stopWatch.lap("redis.writeHash");
        redisTemplateRoute.getTemplate(key).boundHashOps(key).expire(timeOut, timeUnit);
        stopWatch.stop("redis.setHashExpireTime");
    }

    /**
     * 缓存hash空值
     *
     * @param key
     * @param time
     * @param timeUnit
     */
    public void writeHashEmpty(String key, long time, TimeUnit timeUnit) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        logger.debug("返回值为null，缓存空值,key={},time={},timeUnit={}", key, time, timeUnit);
        redisTemplateRoute.getTemplate(key).boundHashOps(key).put(EMPTY_VALUE, EMPTY_VALUE);
        this.setExpireTime(key, time, timeUnit);
        logger.debug("空值写入成功,key={}", key);
        stopWatch.stop("redis.writeHashEmpty");
    }

    /**
     * 读取hash
     *
     * @param key
     * @param beanClass
     * @param <T>
     * @return
     */
    public <T> T loadHash(String key, Class<T> beanClass) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Map loadedHash = redisTemplateRoute.getTemplate(key).boundHashOps(key).entries();
        if (loadedHash.isEmpty() || (loadedHash.containsKey(EMPTY_VALUE) && loadedHash.containsValue(EMPTY_VALUE))) {
            return null;
        }
        Object obj = null;
        try {
            obj = ObjectUtil.convertMap(beanClass, loadedHash);
            stopWatch.lap("redis.loadHash.convertHashToObject");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stopWatch.stop("redis.loadHash");
        }
        return (T) obj;
    }

    /**
     * 判断是否空值
     *
     * @param key
     * @return
     */
    public boolean isEmpty(String key) {
        Map loadedHash = redisTemplateRoute.getTemplate(key).boundHashOps(key).entries();
        if (loadedHash.containsKey(EMPTY_VALUE) && loadedHash.containsValue(EMPTY_VALUE)) {
            return true;
        }
        return false;
    }

    /**
     * 写入list
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    public <T> Long writeList(String key, List<T> value) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        if (!CollectionUtils.isEmpty(value)) {
            if (redisTemplateRoute.getTemplate(key).boundListOps(key).size() < 1) {
                String lockKey = "lock." + key;
                RLock lock = redissonClient.getLock(lockKey);
                try {
                    boolean b = lock.tryLock(redisConfigProperties.getWaitTime(), redisConfigProperties.getLockTime(), TimeUnit.SECONDS);
                    if (b) {
                        //双检锁，以免存入两份章节列表
                        if (redisTemplateRoute.getTemplate(key).boundListOps(key).size() < 1) {
                            return redisTemplateRoute.getTemplate(key).boundListOps(key).leftPushAll(value.toArray());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("get lock exception,{}", e);
                } finally {
                    lock.unlockAsync();
                    stopWatch.stop("redis.writeList");
                }
            }
        }
        return 0L;
    }

    /**
     * 写入list
     *
     * @param key
     * @param value
     * @param <T>
     * @return
     */
    public <T> Long writeRightList(String key, List<T> value) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        if (!CollectionUtils.isEmpty(value)) {
            if (redisTemplateRoute.getTemplate(key).boundListOps(key).size() < 1) {
                String lockKey = "lock." + key;
                RLock lock = redissonClient.getLock(lockKey);
                try {
                    boolean b = lock.tryLock(redisConfigProperties.getWaitTime(), redisConfigProperties.getLockTime(), TimeUnit.SECONDS);
                    if (b) {
                        //双检锁，以免存入两份章节列表
                        if (redisTemplateRoute.getTemplate(key).boundListOps(key).size() < 1) {
                            return redisTemplateRoute.getTemplate(key).boundListOps(key).rightPushAll(value.toArray());
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error("get lock exception,{}", e);
                } finally {
                    lock.unlockAsync();
                    stopWatch.stop("redis.writeList");
                }
            }
        }
        return 0L;
    }

    /**
     * 获取List
     *
     * @param key
     * @param start
     * @param end
     */
    public List loadList(String key, long start, long end) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        List list = redisTemplateRoute.getTemplate(key).opsForList().range(key, start, end);
        stopWatch.stop("redis.loadList");
        return list;
    }

    /**
     * 缓存List空值
     *
     * @param key
     * @param time
     * @param timeUnit
     */
    public Long writeListEmpty(String key, long time, TimeUnit timeUnit, Collection<?> values) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        logger.debug("返回值为null，缓存空值,key={},time={},timeUnit={}", key, time, timeUnit);
        Long result = redisTemplateRoute.getTemplate(key).boundListOps(key).leftPushAll(values);
        this.setExpireTime(key, (int) time, timeUnit);
        logger.debug("空值写入成功,key={}", key);
        stopWatch.stop("redis.writeHashEmpty");
        return result;
    }

    /**
     * 获取所有列表
     *
     * @param key
     * @return
     */
    public List loadListAll(String key) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        List list = redisTemplateRoute.getTemplate(key).opsForList().range(key, 0, -1);
        stopWatch.stop("redis.loadListAll");
        return list;
    }

    /**
     * 删除key
     *
     * @param key
     */
    public void delete(String key) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        redisTemplateRoute.getTemplate(key).delete(key);
        stopWatch.stop("redis.delete");
    }

    /**
     * 删除多个key
     *
     * @param keys
     */
    public void deleteKeys(Collection<String> keys) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        if (!CollectionUtils.isEmpty(keys)) {
            keys.forEach(key -> {
                redisTemplateRoute.getTemplate(key).delete(key);
            });
        }
        stopWatch.stop("redis.deletes");
    }

    /**
     * 获取list长度
     *
     * @param key
     * @return
     */
    public long getListLen(String key) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        Long size = redisTemplateRoute.getTemplate(key).boundListOps(key).size();
        stopWatch.stop("redis.llen");
        return size;
    }

    /**
     * key是否存在
     *
     * @param key
     * @return
     */
    public boolean hasKey(String key) {
        StopWatch stopWatch = new Slf4JStopWatch(cacheLogger);
        boolean flag = redisTemplateRoute.getTemplate(key).hasKey(key);
        stopWatch.stop("redis.hasKey");
        return flag;
    }
}
