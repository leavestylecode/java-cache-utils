package me.leavestyle.handler.str;

import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
public class ArrStrCacheHandler<K, V> extends StrAbstractCacheHandler<K, List<V>, V> {

    /**
     * json映射value的类型
     */
    protected final Class<V> initValueType;

    @Override
    protected Map<K, List<V>> fetchFromCache(List<K> keys) {
        return StrCacheFunUtils.fetchFromCache(keys, this,
                cacheValue -> JsonUtils.toListObjWithDefault(cacheValue, initValueType)
        );
    }

    @Override
    protected Map<K, List<V>> fetchFromDb(List<K> keys) {
        return StrCacheFunUtils.fetchFromDb(keys, this,
                dbData -> dbData.stream().collect(Collectors.groupingBy(reDbGroupFun))
        );
    }

    @Override
    protected void writeToCache(List<K> unCachedKeys, Map<K, List<V>> dbData) {
        StrCacheFunUtils.writeToCache(unCachedKeys, dbData, CollectionUtils::isEmpty, this);
    }

    @Override
    protected List<V> mergeData(List<K> keys, Map<K, List<V>> cachedDb, Map<K, List<V>> dbData) {
        return keys.stream()
                .map(key -> cachedDb.getOrDefault(key, dbData.get(key)))
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
