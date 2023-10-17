package me.leavestyle.utils;

import me.leavestyle.handler.ArrStrCacheHandler;

import java.util.List;
import java.util.function.Function;

public class CacheUtils {

    CacheUtils() {
    }

    /**
     * 缓存默认开关：开启
     */
    private static final Boolean DEFAULT_CACHE_ON = Boolean.TRUE;

    /**
     * 默认缓存时间3000，时间单位取决于使用方
     */
    private static final Long DEFAULT_EXPIRE_TIME = 3000L;

    public static <K, V> ArrStrCacheHandler<K, V> initArrStrCacheHandler(List<K> rawKeys, Function<List<K>, List<V>> dbFun) {
        return new ArrStrCacheHandler<>(rawKeys, DEFAULT_CACHE_ON, dbFun, list -> false, DEFAULT_EXPIRE_TIME);
    }

}
