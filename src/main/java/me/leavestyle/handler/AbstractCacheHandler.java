package me.leavestyle.handler;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
@SuperBuilder(toBuilder = true)
public abstract class AbstractCacheHandler<K, V> {
    @NonNull
    @Builder.Default
    private Boolean cacheOn = Boolean.TRUE;

    /**
     * keys
     */
    @NonNull
    protected final List<K> reKeys;

    /**
     * 不缓存策略，默认关闭
     */
    @NonNull
    @Builder.Default
    protected final Predicate<V> opNoCacheStrategy = obj -> false;

    /**
     * redis过期时间，时间单位和 initCacheBiConsumer 中保持一致
     */
    @NonNull
    @Builder.Default
    protected final Long opExpireTime = 3000L;

    /**
     * redis的key生成函数
     */
    protected final Function<K, String> initRedisKeyFun;

    /**
     * 获取缓存函数
     */
    protected final Function<List<String>, List<String>> initObtainCacheFun;

    /**
     * 缓存数据函数
     */
    protected final BiConsumer<Map<String, String>, Long> initCacheBiConsumer;

    protected abstract List<K> fetchKeys();

    protected abstract Map<K, V> fetchFromCache(List<K> keys);

    protected abstract Map<K, V> fetchFromDb(List<K> keys);

    protected abstract void writeToCache(List<K> unCachedKeys, Map<K, V> dbData);

    protected abstract Map<K, V> mergeData(Result<K, V> result);

    public abstract List<?> handleToList();

    public Map<K, V> handleToMap() {
        return mergeData(handleToResult());
    }

    protected Result<K, V> handleToResult() {
        List<K> keys = fetchKeys();
        if (!cacheOn) {
            log.debug("onCache is false , query form DB");
            return Result.of(keys, new HashMap<>(), fetchFromDb(keys));
        }
        Map<K, V> cacheData = fetchFromCache(keys);
        List<K> uncachedKeys = getUncachedKeys(keys, cacheData);
        Map<K, V> dbData = fetchFromDb(uncachedKeys);
        writeToCache(uncachedKeys, dbData);

        return Result.of(keys, cacheData, dbData);
    }

    @Getter
    @AllArgsConstructor(staticName = "of")
    static class Result<K, V> {
        public final List<K> keys;
        public final Map<K, V> cachedData;
        public final Map<K, V> dbData;
    }

    private List<K> getUncachedKeys(List<K> keys, Map<K, V> cacheMap) {
        return keys.stream().filter(key -> !cacheMap.containsKey(key)).distinct().collect(Collectors.toList());
    }

}
