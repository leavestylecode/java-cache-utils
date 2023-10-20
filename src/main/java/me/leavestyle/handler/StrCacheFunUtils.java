package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import me.leavestyle.common.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class StrCacheFunUtils {

    StrCacheFunUtils() {
    }

    public static <K, V, R> Map<K, V> fetchFromCache(
            List<K> keys, StrAbstractCacheHandler<K, V, R> handler, Function<String, V> convert
    ) {
        List<String> cacheKeys = keys.stream().map(handler.initRedisKeyFun).collect(Collectors.toList());
        List<String> redisValues = handler.initObtainCacheFun.apply(cacheKeys);
        log.debug("Fetch from cache, keys : {}, values : {}", cacheKeys, redisValues);

        return Stream.iterate(0, i -> i + 1).limit(keys.size()).map(i -> {
            String redisValue = redisValues.get(i);
            if (Objects.isNull(redisValue)) {
                return null;
            }
            V value = convert.apply(redisValue);
            if (value == null) {
                return null;
            }
            return Pair.of(keys.get(i), value);
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    public static <K, V, R> Map<K, V> fetchFromDb(
            List<K> keys, StrAbstractCacheHandler<K, V, R> handler, Function<List<R>, Map<K, V>> classifier
    ) {
        if (CollectionUtils.isEmpty(keys)) {
            return new HashMap<>();
        }

        log.debug("query from DB , keys are not cached = {} ", keys);
        List<R> dbData = handler.reDbFun.apply(keys);
        if (CollectionUtils.isEmpty(dbData)) {
            return new HashMap<>();
        }

        // 数据格式转换
        return classifier.apply(dbData);
    }

    public static <K, V, R> void writeToCache(
            List<K> unCachedKeys, Map<K, V> dbData, Predicate<V> emptyStrategy, StrAbstractCacheHandler<K, V, R> handler
    ) {
        if (CollectionUtils.isEmpty(unCachedKeys)) {
            return;
        }
        Map<String, String> convertValues = unCachedKeys.stream().map(key -> {
            V value = dbData.get(key);
            // 不缓存策略
            if (handler.opNoCacheStrategy.test(value)) {
                return null;
            }
            // 空值判断
            if (emptyStrategy.test(value)) {
                return Pair.of(handler.initRedisKeyFun.apply(key), "");
            }
            try {
                return Pair.of(handler.initRedisKeyFun.apply(key), JsonUtils.toJsonStr(value));
            } catch (JsonProcessingException e) {
                log.error("Convert object to json error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        try {
            handler.initCacheBiConsumer.accept(convertValues, handler.opExpireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", JsonUtils.toJsonStr(convertValues.keySet()), handler.opExpireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

}
