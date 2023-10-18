package me.leavestyle.utils;

import me.leavestyle.handler.ArrStrCacheHandler;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class CacheUtilsTest {

    Jedis jedis = new Jedis("localhost", 6379);

    @Test
    void test01() {
        List<String> userIds = Stream.of("1", "2", "3").collect(Collectors.toList());

        List<User> users = ArrStrCacheHandler.<String, User>builder()
                .reRawKeys(userIds)
                .reDbFun(CacheUtilsTest::dbFun)
                .initRedisKeyFun(CacheUtilsTest::toKeyStr)
                .initObtainCacheFun(this::mGet)
                .initDbKeyFun(User::getUserId)
                .initCacheBiConsumer(this::cacheData)
                .build().handle();

        users.forEach(System.out::println);
    }

    private static String toKeyStr(String i) {
        return "test-key:" + i;
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
            String value = jedis.get(String.valueOf(keys));
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
