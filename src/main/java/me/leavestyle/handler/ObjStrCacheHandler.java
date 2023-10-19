package me.leavestyle.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 批量缓存处理：缓存值为数组格式的字符串。
 * 即缓存key对应1个value的场景。
 * 缓存中的value格式为：{obj}
 *
 * @param <K> key的基础类型
 * @param <R> value的基础类型
 */
@Slf4j
@SuperBuilder(toBuilder = true)
public class ObjStrCacheHandler<K, R> extends AbstractCacheHandler<K, R> {


    /**
     * keys
     */
    @NonNull
    private final List<K> reKeys;

    /**
     * DB查询函数
     */
    @NonNull
    private final Function<List<K>, List<R>> reDbFun;

    /**
     * DB的key取值函数
     */
    private final Function<R, K> reDbGroupFun;

    /**
     * 不缓存策略，默认关闭
     */
    @NonNull
    @Builder.Default
    private final Predicate<R> opNoCacheStrategy = object -> false;

    /**
     * redis过期时间，时间单位和 initCacheBiConsumer 中保持一致
     */
    @NonNull
    @Builder.Default
    private final Long opExpireTime = 3000L;

    /**
     * redis的key生成函数
     */
    private final Function<K, String> initRedisKeyFun;

    /**
     * 获取缓存函数
     */
    private final Function<List<String>, List<String>> initObtainCacheFun;

    /**
     * 缓存数据函数
     */
    private final BiConsumer<Map<String, String>, Long> initCacheBiConsumer;

    @Override
    protected List<K> fetchKeys() {
        return this.reKeys.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    protected Map<K, R> fetchFromCache(List<K> keys) {
        List<String> cacheKeys = keys.stream().map(initRedisKeyFun).collect(Collectors.toList());
        List<String> redisValues = initObtainCacheFun.apply(cacheKeys);

        return Stream.iterate(0, i -> i + 1).limit(keys.size()).map(i -> {
            String redisValue = redisValues.get(i);
            if (Objects.isNull(redisValue)) {
                return null;
            }
            try {
                R value = StringUtils.isBlank(redisValue) ? null : new ObjectMapper().readValue(redisValue, new TypeReference<R>() {
                });
                return Pair.of(keys.get(i), value);
            } catch (IOException e) {
                log.error("Convert json to object error : ", e);
                return null;
            }
        }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    }

    @Override
    protected Map<K, R> fetchFromDb(List<K> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new HashMap<>();
        }

        log.debug("query from DB , keys are not cached = {} ", keys);
        List<R> dbData = this.reDbFun.apply(keys);
        if (CollectionUtils.isEmpty(dbData)) {
            return new HashMap<>();
        }

        // 数据格式转换
        return dbData.stream().collect(Collectors.toMap(this.reDbGroupFun, i -> i));
    }

    @Override
    protected void writeToCache(List<K> unCachedKeys, Map<K, R> dbData) {
        if (CollectionUtils.isEmpty(unCachedKeys)) {
            return;
        }
        try {
            Map<String, String> convertValues = unCachedKeys.stream().map(key -> {
                R value = dbData.get(key);
                // 不缓存策略
                if (this.opNoCacheStrategy.test(value)) {
                    return null;
                }
                // 空值处理
                if (value == null) {
                    return Pair.of(this.initRedisKeyFun.apply(key), "");
                }
                try {
                    return Pair.of(this.initRedisKeyFun.apply(key), new ObjectMapper().writeValueAsString(value));
                } catch (JsonProcessingException e) {
                    log.error("Convert object to json error : ", e);
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            this.initCacheBiConsumer.accept(convertValues, this.opExpireTime);
            log.info("multiSet cache, keys = {} expireMs = {}", new ObjectMapper().writeValueAsString(convertValues.keySet()), this.opExpireTime);
        } catch (Exception e) {
            log.error("redis multi cache error : ", e);
        }
    }

    @Override
    protected Map<K, R> mergeData(AbstractCacheHandler.Result<K, R> result) {
        Map<K, R> cachedData = result.getCachedData();
        Map<K, R> dbData = result.getDbData();

        return Stream.of(cachedData, dbData).flatMap(map -> map.entrySet().stream()).filter(entry ->
                entry != null && entry.getValue() != null
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public List<R> handleToList() {
        Map<K, R> data = this.handleToMap();
        return reKeys.stream().map(data::get).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
