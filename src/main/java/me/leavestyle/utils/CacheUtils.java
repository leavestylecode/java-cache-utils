package me.leavestyle.utils;

import me.leavestyle.builder.MultiCacheBuilder;

import java.util.List;
import java.util.function.Function;

public class CacheUtils {

    public static <K, V> MultiCacheBuilder<K, V> initMultiCacheBuilder(Function<List<K>, List<V>> dbFun) {
        return new MultiCacheBuilder<>(dbFun);
    }

}
