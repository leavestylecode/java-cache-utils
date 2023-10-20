package me.leavestyle.utils;

import me.leavestyle.handler.str.ArrStrCacheHandler;
import me.leavestyle.handler.str.ObjStrCacheHandler;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheTest {

    Jedis jedis = new Jedis("localhost", 6379);

    @Test
    void testArrStr() {
        List<String> ids = Stream.of("1", "2", "3").collect(Collectors.toList());

        ArrStrCacheHandler<String, Model> arrStrCacheHandler = ArrStrCacheHandler.<String, Model>builder()
                .reDbFun(CacheTest::dbFun)
                .reDbGroupFun(Model::getId)
                .initValueType(Model.class)
                .initRedisKeyFun(i -> "key-arr:" + i)
                .initObtainCacheFun(this::mGet)
                .initCacheBiConsumer(this::cacheData)
                .opExpireTime(30000L)
                .build();

        // 测试缓存
        List<Model> cacheModelsList = arrStrCacheHandler.handle(ids);

        assertEquals(cacheModelsList.size(), ids.size());
        assertTrue(cacheModelsList.stream().allMatch(model -> ids.stream().anyMatch(id -> id.equals(model.getId()))));

        // 测试DB
        List<Model> dbModelsList = arrStrCacheHandler.toBuilder().cacheOn(Boolean.FALSE).build().handle(ids);
        assertEquals(dbModelsList.size(), ids.size());
        assertTrue(dbModelsList.stream().allMatch(model -> ids.contains(model.getId())));
    }

    @Test
    void testObjStr() {
        List<String> ids = Stream.of("3", "4", "5").collect(Collectors.toList());
        ObjStrCacheHandler<String, Model> arrStrCacheHandler = ObjStrCacheHandler.<String, Model>builder()
                .reDbFun(CacheTest::dbFun)
                .reDbGroupFun(Model::getId)
                .initValueType(Model.class)
                .initRedisKeyFun(i -> "key-obj:" + i)
                .initObtainCacheFun(this::mGet)
                .initCacheBiConsumer(this::cacheData)
                .opExpireTime(30000L)
                .build();

        // 测试缓存
        List<Model> cacheModelsList = arrStrCacheHandler.handle(ids);
        assertEquals(cacheModelsList.size(), ids.size());
        assertTrue(cacheModelsList.stream().allMatch(model -> ids.contains(model.getId())));

        // 测试DB
        List<Model> dbModelsList = arrStrCacheHandler.toBuilder().cacheOn(Boolean.FALSE).build().handle(ids);
        assertEquals(dbModelsList.size(), ids.size());
        assertTrue(dbModelsList.stream().allMatch(model -> ids.contains(model.getId())));

    }

    private static List<Model> dbFun(List<String> keys) {
        return Stream.of(
                Model.builder().id("1").name("name1").status("status1").build(),
                Model.builder().id("2").name("name2").status("status2").build(),
                Model.builder().id("3").name("name3").status("status3").build(),
                Model.builder().id("4").name("name4").status("status4").build(),
                Model.builder().id("5").name("name5").status("status5").build()
        ).filter(model -> keys.contains(model.getId())).collect(Collectors.toList());
    }

    private List<String> mGet(List<String> keys) {
        return keys.stream().map(key -> {
            String value = jedis.get(String.valueOf(key));
            if (value == null || value.equals("nil")) {
                return null;
            }
            return value;
        }).collect(Collectors.toList());
    }

    private void cacheData(Map<String, String> dataMap, Long expireTime) {
        dataMap.forEach((key, value) -> {
            jedis.set(key, value);
            jedis.expire(key, expireTime);
        });
    }

}
