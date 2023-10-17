package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ArrStrCacheHandler<K, V> {

    /**
     * 缓存开关，默认开启
     */
    @NonNull
    private Boolean cacheOn;

    /**
     * keys
     */
    @NonNull
    private final List<K> rawKeys;

    /**
     * DB查询函数
     */
    @NonNull
    private final Function<List<K>, List<V>> dbFun;

    /**
     * 不缓存策略，默认关闭
     */
    @NonNull
    private Predicate<List<V>> noCacheStrategy;

    /**
     * redis过期时间，时间单位和 putCacheConsumer 中保持一致
     */
    @NonNull
    private Long expireTime;

    public ArrStrCacheHandler(
            @NonNull List<K> rawKeys, @NonNull Boolean onCache, @NonNull Function<List<K>, List<V>> dbFun,
            @NonNull Predicate<List<V>> noCacheStrategy, @NonNull Long expireTime
    ) {
        this.rawKeys = rawKeys;
        this.cacheOn = onCache;
        this.dbFun = dbFun;
        this.noCacheStrategy = noCacheStrategy;
        this.expireTime = expireTime;
    }


    /**
     * redis的key生成函数
     */
    private Function<K, String> redisKeyFun;

    /**
     * 获取缓存函数
     */
    private Function<List<String>, List<String>> obtainCacheFun;

    /**
     * 缓存数据函数
     */
    private BiConsumer<Map<String, String>, Long> cacheBiConsumer;

    /**
     * DB的key取值函数
     */
    private Function<V, K> dbKeyFun;

    public ArrStrCacheHandler<K, V> cacheOn() {
        this.cacheOn = Boolean.TRUE;
        return this;
    }

    public ArrStrCacheHandler<K, V> cacheOff() {
        this.cacheOn = Boolean.FALSE;
        return this;
    }

    public ArrStrCacheHandler<K, V> cacheOnSupp(@NonNull BooleanSupplier supplier) {
        this.cacheOn = supplier.getAsBoolean();
        return this;
    }

    public ArrStrCacheHandler<K, V> initObtainCacheHandle(UnaryOperator<List<String>> obtainCacheFun) {
        this.obtainCacheFun = obtainCacheFun;
        return this;
    }

    public ArrStrCacheHandler<K, V> initCacheHandle(BiConsumer<Map<String, String>, Long> cacheBiConsumer) {
        this.cacheBiConsumer = cacheBiConsumer;
        return this;
    }

    public ArrStrCacheHandler<K, V> initDbKeyHandle(Function<V, K> dbKeyFun) {
        this.dbKeyFun = dbKeyFun;
        return this;
    }

    public ArrStrCacheHandler<K, V> initRedisKeyHandle(Function<K, String> redisKeyFun) {
        this.redisKeyFun = redisKeyFun;
        return this;
    }

    public ArrStrCacheHandler<K, V> opNoCacheStrategyHandle(@NonNull Predicate<List<V>> nocacheStrategy) {
        this.noCacheStrategy = nocacheStrategy;
        return this;
    }

    public ArrStrCacheHandler<K, V> opExpireTime(@NonNull Long expireTime) {
        this.expireTime = expireTime;
        return this;
    }

    public List<V> handle() {
        // 开关关闭，执行DB
        if (!cacheOn) {
            log.debug("onCache is false , query form DB");
            return dbFun.apply(rawKeys);
        }
        // 关键缓存参数不存在，执行DB
        if (obtainCacheFun == null || redisKeyFun == null || dbKeyFun == null || noCacheStrategy == null) {
            log.debug("cache param is invalid , query form DB");
            return dbFun.apply(rawKeys);
        }
        // 校验和过滤key
        List<K> filterRawKeys = rawKeys.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filterRawKeys)) {
            return new ArrayList<>();
        }
        // 查缓存
        Map<K, List<V>> cachedData = queryCache(filterRawKeys, redisKeyFun);
        // 未缓存的key
        List<K> noCachedKeys = toNoCachedKeys(filterRawKeys, cachedData);
        // 读库
        Map<K, List<V>> dbData = getMultiDataFromDb(noCachedKeys, dbFun, dbKeyFun);
        // 缓存
        cacheMultiData(noCachedKeys, redisKeyFun, dbData, noCacheStrategy, expireTime);
        // 按顺序合并
        return concat(filterRawKeys, cachedData, dbData);
    }

    private <V, K> List<V> concat(List<K> rawKeys, Map<K, List<V>> cachedData, Map<K, List<V>> dbData) {
        return rawKeys.stream().map(rawKey ->
                cachedData.getOrDefault(rawKey, dbData.getOrDefault(rawKey, new ArrayList<>()))
        ).flatMap(List::stream).collect(Collectors.toList());
    }

    private <K, V> Map<K, List<V>> queryCache(List<K> rawKeys, Function<K, String> redisKeyFun) {
        List<String> keys = rawKeys.stream().filter(Objects::nonNull).map(redisKeyFun).collect(Collectors.toList());
        List<String> redisValues = obtainCacheFun.apply(keys);

        return Stream.iterate(0, i -> i + 1).limit(keys.size()).map(i -> {
            String redisValue = redisValues.get(i);
            if (Objects.isNull(redisValue)) {
                return null;
            }
            try {
                List<V> value = StringUtils.isBlank(redisValue) ? new ArrayList<>() : new ObjectMapper().readValue(redisValue, new TypeReference<List<V>>() {
                });
                return Pair.of(rawKeys.get(i), value);
            } catch (IOException e) {
                log.error("Convert json to object error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    /**
     * 通用批量缓存
     *
     * @param keys            自定义key，比如：userId, tagId等等
     * @param redisKeyFun     redis的Key处理函数：K -> String
     * @param kValues         数据对
     * @param nocacheStrategy 不缓存策略
     * @param expireTime      redis缓存时间（毫秒）
     * @param <K>             自定义Key类型
     * @param <V>             自定义Value类型
     */
    private <K, V> void cacheMultiData(
            List<K> keys, Function<K, String> redisKeyFun, Map<K, List<V>> kValues,
            Predicate<List<V>> nocacheStrategy, Long expireTime
    ) {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        try {
            Map<String, String> convertValues = keys.stream().map(key -> {
                List<V> value = kValues.get(key);
                // 不缓存策略
                if (nocacheStrategy.test(value)) {
                    return null;
                }
                // 空值处理
                if (CollectionUtils.isEmpty(value)) {
                    return Pair.of(redisKeyFun.apply(key), "");
                }
                try {
                    return Pair.of(redisKeyFun.apply(key), new ObjectMapper().writeValueAsString(value));
                } catch (JsonProcessingException e) {
                    log.error("Convert object to json error : ", e);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            cacheBiConsumer.accept(convertValues, expireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", new ObjectMapper().writeValueAsString(convertValues.keySet()), expireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

    /**
     * 从库中查询缓存未命中的用户地址，并进行数据转换
     *
     * @param noCachedKeys 未被缓存的用户id
     * @return 被分类的用户地址，key是user_id，value是允许key重复的地址列表
     */
    private <K, V> Map<K, List<V>> getMultiDataFromDb(
            List<K> noCachedKeys, Function<List<K>, List<V>> dbFun, Function<V, K> groupFun
    ) {
        if (CollectionUtils.isEmpty(noCachedKeys)) {
            return new HashMap<>();
        }

        log.debug("query from DB , keys are not cached = {} ", noCachedKeys);
        List<V> dbData = dbFun.apply(noCachedKeys);
        if (CollectionUtils.isEmpty(dbData)) {
            return new HashMap<>();
        }

        // 数据格式转换
        return dbData.stream().collect(Collectors.groupingBy(groupFun));
    }

    /**
     * 拿到未被缓存的key
     */
    private <K, V> List<K> toNoCachedKeys(List<K> rawKeys, Map<K, List<V>> cacheMap) {
        return rawKeys.stream().filter(k -> !cacheMap.containsKey(k)).distinct().collect(Collectors.toList());
    }

}
