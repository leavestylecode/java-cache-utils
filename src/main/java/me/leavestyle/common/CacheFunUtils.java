package me.leavestyle.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
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

    public static void writeToCache(
            ObjLongConsumer<Map<String, String>> initCacheBiConsumer, Map<String, String> convertValues, Long opExpireTime
    ) {
        if (MapUtils.isEmpty(convertValues)) {
            return;
        }
        try {
            initCacheBiConsumer.accept(convertValues, opExpireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", JsonUtils.toJsonStr(convertValues.keySet()), opExpireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

}
