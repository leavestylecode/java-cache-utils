package me.leavestyle.utils;

import me.leavestyle.handler.ArrStrCacheHandler;
import me.leavestyle.handler.ObjStrCacheHandler;
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
        List<String> userIds = Stream.of("1", "2", "3").collect(Collectors.toList());

        ArrStrCacheHandler<String, User> arrStrCacheHandler = ArrStrCacheHandler.<String, User>builder()
                .reKeys(userIds)
                .reDbFun(CacheTest::dbFun)
                .reDbGroupFun(User::getUserId)
                .initValueType(User.class)
                .initRedisKeyFun(i -> "test-key-arr:" + i)
                .initObtainCacheFun(this::mGet)
                .initCacheBiConsumer(this::cacheData)
                .opExpireTime(30000L)
                .build();

        // 测试缓存
        List<User> cacheUsersList = arrStrCacheHandler.handleToList();

        assertEquals(cacheUsersList.size(), userIds.size());
        assertTrue(cacheUsersList.stream().allMatch(user -> userIds.stream().anyMatch(id -> id.equals(user.getUserId()))));

        Map<String, List<User>> cacheUsersMap = arrStrCacheHandler.handleToMap();
        assertEquals(cacheUsersMap.size(), userIds.size());
        assertTrue(cacheUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getKey())));
        assertTrue(cacheUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getValue().get(0).getUserId())));

        // 测试DB
        List<User> dbUsersList = arrStrCacheHandler.toBuilder().cacheOn(Boolean.FALSE).build().handleToList();
        assertEquals(dbUsersList.size(), userIds.size());
        assertTrue(dbUsersList.stream().allMatch(user -> userIds.contains(user.getUserId())));

        Map<String, List<User>> dbUsersMap = arrStrCacheHandler.handleToMap();
        assertEquals(dbUsersMap.size(), userIds.size());
        assertTrue(dbUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getKey())));
        assertTrue(dbUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getValue().get(0).getUserId())));
    }

    @Test
    void testObjStr() {
        List<String> userIds = Stream.of("3", "4", "5").collect(Collectors.toList());
        ObjStrCacheHandler<String, User> arrStrCacheHandler = ObjStrCacheHandler.<String, User>builder()
                .reKeys(userIds)
                .reDbFun(CacheTest::dbFun)
                .reDbGroupFun(User::getUserId)
                .initValueType(User.class)
                .initRedisKeyFun(i -> "test-key-obj:" + i)
                .initObtainCacheFun(this::mGet)
                .initCacheBiConsumer(this::cacheData)
                .opExpireTime(30000L)
                .build();

        // 测试缓存
        List<User> cacheUsersList = arrStrCacheHandler.handleToList();
        assertEquals(cacheUsersList.size(), userIds.size());
        assertTrue(cacheUsersList.stream().allMatch(user -> userIds.contains(user.getUserId())));

        Map<String, User> cacheUsersMap = arrStrCacheHandler.handleToMap();
        assertEquals(cacheUsersMap.size(), userIds.size());
        assertTrue(cacheUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getKey())));
        assertTrue(cacheUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getValue().getUserId())));

        // 测试DB
        List<User> dbUsersList = arrStrCacheHandler.toBuilder().cacheOn(Boolean.FALSE).build().handleToList();
        assertEquals(dbUsersList.size(), userIds.size());
        assertTrue(dbUsersList.stream().allMatch(user -> userIds.contains(user.getUserId())));

        Map<String, User> dbUsersMap = arrStrCacheHandler.handleToMap();
        assertEquals(dbUsersMap.size(), userIds.size());
        assertTrue(dbUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getKey())));
        assertTrue(dbUsersMap.entrySet().stream().allMatch(entry -> userIds.contains(entry.getValue().getUserId())));

    }

    private static List<User> dbFun(List<String> keys) {
        return Stream.of(
                User.builder().userId("1").userName("name1").userAddress("address1").build(),
                User.builder().userId("2").userName("name2").userAddress("address2").build(),
                User.builder().userId("3").userName("name3").userAddress("address3").build(),
                User.builder().userId("4").userName("name4").userAddress("address4").build(),
                User.builder().userId("5").userName("name5").userAddress("address5").build()
        ).filter(user -> keys.contains(user.getUserId())).collect(Collectors.toList());
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
