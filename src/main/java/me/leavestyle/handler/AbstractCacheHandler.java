package me.leavestyle.handler;


import lombok.Builder;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@SuperBuilder(toBuilder = true)
public abstract class AbstractCacheHandler<K, V, R> {

    @NonNull
    @Builder.Default
    private Boolean cacheOn = Boolean.TRUE;

    protected abstract Map<K, V> fetchFromCache(List<K> keys);

    protected abstract Map<K, V> fetchFromDb(List<K> keys);

    protected abstract void writeToCache(List<K> unCachedKeys, Map<K, V> dbData);

    protected abstract List<R> mergeData(List<K> keys, Map<K, V> cachedDb, Map<K, V> dbData);

    public List<R> handle(List<K> keys) {
        List<K> filterKeys = keys.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (!cacheOn) {
            log.debug("cacheOn is false, query DB");
            return mergeData(filterKeys, new HashMap<>(), fetchFromCache(filterKeys));
        }
        Map<K, V> cachedData = fetchFromCache(filterKeys);
        List<K> unCachedKeys = toUncachedKeys(filterKeys, cachedData);
        Map<K, V> dbData = fetchFromDb(unCachedKeys);
        writeToCache(unCachedKeys, dbData);

        return mergeData(filterKeys, cachedData, dbData);
    }

    private List<K> toUncachedKeys(List<K> keys, Map<K, V> cacheMap) {
        return keys.stream().filter(key -> !cacheMap.containsKey(key)).distinct().collect(Collectors.toList());
    }

}
