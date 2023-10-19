package me.leavestyle.handler;

import lombok.Builder;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
public abstract class StrAbstractCacheHandler<K, V, R> extends AbstractCacheHandler<K, V, R> {

    /**
     * DB查询函数
     */
    @NonNull
    protected final Function<List<K>, List<R>> reDbFun;

    /**
     * DB的key取值函数
     */
    protected final Function<R, K> reDbGroupFun;

    /**
     * redis的key生成函数
     */
    protected final Function<K, String> initRedisKeyFun;

    /**
     * 获取缓存函数
     */
    protected final Function<List<String>, List<String>> initObtainCacheFun;

    /**
     * 缓存数据函数
     */
    protected final BiConsumer<Map<String, String>, Long> initCacheBiConsumer;

    /**
     * 不缓存策略，默认关闭
     */
    @NonNull
    @Builder.Default
    protected final Predicate<V> opNoCacheStrategy = obj -> false;

    /**
     * redis过期时间，时间单位和 initCacheBiConsumer 中保持一致
     */
    @NonNull
    @Builder.Default
    protected final Long opExpireTime = 3000L;


}
