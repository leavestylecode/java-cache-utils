package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<String> cacheKeys = keys.stream().map(this.initRedisKeyFun).collect(Collectors.toList());
        List<String> redisValues = initObtainCacheFun.apply(cacheKeys);
        log.debug("Fetch from cache, keys : {}, values : {}", cacheKeys, redisValues);

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
        if (MapUtils.isEmpty(dbData)) {
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
    protected List<V> mergeData(List<K> keys, Map<K, List<V>> cachedDb, Map<K, List<V>> dbData) {
        return keys.stream()
                .map(key -> cachedDb.getOrDefault(key, dbData.get(key)))
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
