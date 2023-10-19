package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.CacheFunUtils;
import me.leavestyle.common.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

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
        if (CollectionUtils.isEmpty(unCachedKeys)) {
            return;
        }
        Map<String, String> convertValues = unCachedKeys.stream().map(key -> {
            V value = dbData.get(key);
            // 不缓存策略
            if (this.opNoCacheStrategy.test(value)) {
                return null;
            }
            // 空值处理
            if (value == null) {
                return Pair.of(this.initRedisKeyFun.apply(key), "");
            }
            try {
                return Pair.of(this.initRedisKeyFun.apply(key), JsonUtils.toJsonStr(value));
            } catch (JsonProcessingException e) {
                log.error("Convert object to json error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        // 写入缓存
        CacheFunUtils.writeToCache(initCacheBiConsumer, convertValues, opExpireTime);
    }

    @Override
    protected List<V> mergeData(List<K> keys, Map<K, V> cachedDb, Map<K, V> dbData) {
        return keys.stream()
                .map(key -> cachedDb.getOrDefault(key, dbData.get(key)))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
