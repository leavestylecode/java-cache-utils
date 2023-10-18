package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Builder
public class ArrStrCacheHandler<K, V> {

    /**
     * 缓存开关，默认开启
     */
    @NonNull
    @Builder.Default
    private Boolean cacheOn = Boolean.TRUE;

    /**
     * keys
     */
    @NonNull
    private final List<K> reRawKeys;

    /**
     * DB查询函数
     */
    @NonNull
    private final Function<List<K>, List<V>> reDbFun;

    /**
     * 不缓存策略，默认关闭
     */
    @NonNull
    @Builder.Default
    private final Predicate<List<V>> opNoCacheStrategy = list -> false;

    /**
     * redis过期时间，时间单位和 putCacheConsumer 中保持一致
     */
    @NonNull
    @Builder.Default
    private final Long opExpireTime = 3000L;


    /**
     * redis的key生成函数
     */
    private final Function<K, String> initRedisKeyFun;

    /**
     * 获取缓存函数
     */
    private final Function<List<String>, List<String>> initObtainCacheFun;

    /**
     * 缓存数据函数
     */
    private final BiConsumer<Map<String, String>, Long> initCacheBiConsumer;

    /**
     * DB的key取值函数
     */
    private final Function<V, K> initDbKeyFun;

    public List<V> handle() {
        // 开关关闭，执行DB
        if (!cacheOn) {
            log.debug("onCache is false , query form DB");
            return reDbFun.apply(reRawKeys);
        }
        // 关键缓存参数不存在，执行DB
        if (initObtainCacheFun == null || initRedisKeyFun == null || initDbKeyFun == null) {
            log.debug("cache param is invalid , query form DB");
            return reDbFun.apply(reRawKeys);
        }
        // 校验和过滤key
        List<K> filterRawKeys = reRawKeys.stream().filter(Objects::nonNull).distinct().collect(Collectors.toList());
        if (CollectionUtils.isEmpty(filterRawKeys)) {
            return new ArrayList<>();
        }
        // 查缓存
        Map<K, List<V>> cachedData = queryCache(filterRawKeys, initRedisKeyFun);
        // 未缓存的key
        List<K> noCachedKeys = toNoCachedKeys(filterRawKeys, cachedData);
        // 读库
        Map<K, List<V>> dbData = getMultiDataFromDb(noCachedKeys, reDbFun, initDbKeyFun);
        // 缓存
        cacheMultiData(noCachedKeys, initRedisKeyFun, dbData, opNoCacheStrategy, opExpireTime);
        // 按顺序合并
        return concat(filterRawKeys, cachedData, dbData);
    }

    private List<V> concat(List<K> rawKeys, Map<K, List<V>> cachedData, Map<K, List<V>> dbData) {
        return rawKeys.stream().map(rawKey ->
                cachedData.getOrDefault(rawKey, dbData.getOrDefault(rawKey, new ArrayList<>()))
        ).flatMap(List::stream).collect(Collectors.toList());
    }

    private Map<K, List<V>> queryCache(List<K> rawKeys, Function<K, String> redisKeyFun) {
        List<String> keys = rawKeys.stream().filter(Objects::nonNull).map(redisKeyFun).collect(Collectors.toList());
        List<String> redisValues = initObtainCacheFun.apply(keys);

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
     */
    private void cacheMultiData(
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
            initCacheBiConsumer.accept(convertValues, expireTime);
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
    private Map<K, List<V>> getMultiDataFromDb(
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
    private List<K> toNoCachedKeys(List<K> rawKeys, Map<K, List<V>> cacheMap) {
        return rawKeys.stream().filter(k -> !cacheMap.containsKey(k)).distinct().collect(Collectors.toList());
    }
}
