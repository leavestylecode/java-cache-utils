package me.leavestyle.handler;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.CacheFunUtils;
import me.leavestyle.common.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 批量缓存处理：缓存值为数组格式的字符串。
 * 即缓存key对应1个value的场景。
 * 缓存中的value格式为：{obj}
 *
 * @param <K> key的基础类型
 * @param <V> value的基础类型
 */
@Slf4j
@SuperBuilder(toBuilder = true)
public class ObjStrCacheHandler<K, V> extends StrAbstractCacheHandler<K, V, V> {

    /**
     * json映射value的类型
     */
    protected final Class<V> initValueType;

    @Override
    protected Map<K, V> fetchFromCache(List<K> keys) {
        return CacheFunUtils.fetchFromCache(keys, initRedisKeyFun, initObtainCacheFun,
                cacheValue -> JsonUtils.toObjWithDefault(cacheValue, initValueType)
        );
    }

    @Override
    protected Map<K, V> fetchFromDb(List<K> keys) {
        return CacheFunUtils.fetchFromDb(keys, reDbFun,
                dbData -> dbData.stream().collect(Collectors.toMap(this.reDbGroupFun, i -> i))
        );
    }

    @Override
    protected void writeToCache(List<K> unCachedKeys, Map<K, V> dbData) {
        CacheFunUtils.writeToCache(unCachedKeys, dbData, this.opNoCacheStrategy, Objects::isNull,
                this.initRedisKeyFun, initCacheBiConsumer, opExpireTime
        );
    }

    @Override
    protected List<V> mergeData(List<K> keys, Map<K, V> cachedDb, Map<K, V> dbData) {
        return keys.stream()
                .map(key -> cachedDb.getOrDefault(key, dbData.get(key)))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
