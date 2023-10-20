package me.leavestyle.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CacheFunUtils {

    CacheFunUtils() {
    }

    public static <K, R> Map<K, R> fetchFromCache(
            List<K> keys, Function<K, String> initRedisKeyFun, UnaryOperator<List<String>> initObtainCacheFun,
            Function<String, R> convert
    ) {
        List<String> cacheKeys = keys.stream().map(initRedisKeyFun).collect(Collectors.toList());
        List<String> redisValues = initObtainCacheFun.apply(cacheKeys);
        log.debug("Fetch from cache, keys : {}, values : {}", cacheKeys, redisValues);

        return Stream.iterate(0, i -> i + 1).limit(keys.size()).map(i -> {
            String redisValue = redisValues.get(i);
            if (Objects.isNull(redisValue)) {
                return null;
            }
            R value = convert.apply(redisValue);
            if (value == null) {
                return null;
            }
            return Pair.of(keys.get(i), value);
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    public static <K, V, R> Map<K, R> fetchFromDb(
            List<K> keys, Function<List<K>, List<V>> reDbFun, Function<List<V>, Map<K, R>> classifier
    ) {
        if (CollectionUtils.isEmpty(keys)) {
            return new HashMap<>();
        }

        log.debug("query from DB , keys are not cached = {} ", keys);
        List<V> dbData = reDbFun.apply(keys);
        if (CollectionUtils.isEmpty(dbData)) {
            return new HashMap<>();
        }

        // 数据格式转换
        return classifier.apply(dbData);
    }

    public static <K, V> void writeToCache(
            List<K> unCachedKeys, Map<K, V> dbData, Predicate<V> opNoCacheStrategy, Predicate<V> emptyStrategy,
            Function<K, String> initRedisKeyFun, ObjLongConsumer<Map<String, String>> initCacheBiConsumer, Long opExpireTime
    ) {
        if (CollectionUtils.isEmpty(unCachedKeys)) {
            return;
        }
        Map<String, String> convertValues = unCachedKeys.stream().map(key -> {
            V value = dbData.get(key);
            // 不缓存策略
            if (opNoCacheStrategy.test(value)) {
                return null;
            }
            // 空值判断
            if (emptyStrategy.test(value)) {
                return Pair.of(initRedisKeyFun.apply(key), "");
            }
            try {
                return Pair.of(initRedisKeyFun.apply(key), JsonUtils.toJsonStr(value));
            } catch (JsonProcessingException e) {
                log.error("Convert object to json error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        try {
            initCacheBiConsumer.accept(convertValues, opExpireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", JsonUtils.toJsonStr(convertValues.keySet()), opExpireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

}
