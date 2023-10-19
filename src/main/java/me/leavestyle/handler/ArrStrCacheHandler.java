package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 批量缓存处理：缓存值为数组格式的字符串。
 * 即缓存key对应1-n个value的场景。
 * 缓存中的value格式为：[{obj1},{obj2}]
 *
 * @param <K> key的基础类型
 * @param <V> value的基础类型
 */
@Slf4j
@SuperBuilder(toBuilder = true)
public class ArrStrCacheHandler<K, V> extends AbstractCacheHandler<K, List<V>> {

    /**
     * DB查询函数
     */
    @NonNull
    private final Function<List<K>, List<V>> reDbFun;

    /**
     * DB的key取值函数
     */
    protected final Function<V, K> reDbGroupFun;

    /**
     * json映射value的类型
     */
    protected final Class<V> initValueType;

    @Override
    protected Map<K, List<V>> fetchFromCache(List<K> keys) {
        List<String> cacheKeys = keys.stream().map(this.initRedisKeyFun).collect(Collectors.toList());
        List<String> redisValues = initObtainCacheFun.apply(cacheKeys);

        return Stream.iterate(0, i -> i + 1).limit(keys.size()).map(i -> {
            String redisValue = redisValues.get(i);
            if (Objects.isNull(redisValue)) {
                return null;
            }
            try {
                List<V> value = StringUtils.isBlank(redisValue) ? new ArrayList<>() : JsonUtils.toListObj(redisValue, initValueType);
                return Pair.of(keys.get(i), value);
            } catch (IOException e) {
                log.error("Convert json to object error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    @Override
    protected Map<K, List<V>> fetchFromDb(List<K> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new HashMap<>();
        }

        log.debug("query from DB , keys are not cached = {} ", keys);
        List<V> dbData = this.reDbFun.apply(keys);
        if (CollectionUtils.isEmpty(dbData)) {
            return new HashMap<>();
        }

        // 数据格式转换
        return dbData.stream().collect(Collectors.groupingBy(reDbGroupFun));
    }

    @Override
    protected void writeToCache(List<K> unCachedKeys, Map<K, List<V>> dbData) {
        if (CollectionUtils.isEmpty(unCachedKeys)) {
            return;
        }
        try {
            Map<String, String> convertValues = unCachedKeys.stream().map(key -> {
                List<V> value = dbData.get(key);
                // 不缓存策略
                if (this.opNoCacheStrategy.test(value)) {
                    return null;
                }
                // 空值处理
                if (CollectionUtils.isEmpty(value)) {
                    return Pair.of(this.initRedisKeyFun.apply(key), "");
                }
                try {
                    return Pair.of(this.initRedisKeyFun.apply(key), JsonUtils.toJsonStr(value));
                } catch (JsonProcessingException e) {
                    log.error("Convert object to json error : ", e);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            this.initCacheBiConsumer.accept(convertValues, this.opExpireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", JsonUtils.toJsonStr(convertValues.keySet()), this.opExpireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

    @Override
    protected Map<K, List<V>> mergeData(AbstractCacheHandler.Result<K, List<V>> result) {
        Map<K, List<V>> cachedData = result.getCachedData();
        Map<K, List<V>> dbData = result.getDbData();

        return Stream.of(cachedData, dbData).flatMap(map -> map.entrySet().stream()).filter(entry ->
                entry != null && CollectionUtils.isNotEmpty(entry.getValue())
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public List<V> handleToList(List<K> keys) {
        Map<K, List<V>> data = this.handleToMap(keys);
        return keys.stream().map(data::get).filter(CollectionUtils::isNotEmpty).flatMap(Collection::stream).collect(Collectors.toList());
    }

}
