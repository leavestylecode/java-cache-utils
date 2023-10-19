package me.leavestyle.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@SuppressWarnings("java:S1168")
public class JsonUtils {

    JsonUtils() {
    }

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> List<T> toListObj(String json, Class<T> type) throws JsonProcessingException {
        CollectionType collectionType = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, type);
        return OBJECT_MAPPER.readValue(json, collectionType);
    }

    public static <T> T toObj(String json, Class<T> type) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, type);
    }

    public static String toJsonStr(Object obj) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(obj);
    }

    public static <T> List<T> toListObjWithDefault(String json, Class<T> type) {
        try {
            return toListObj(json, type);
        } catch (JsonProcessingException e) {
            logError(json);
            return null;
        }
    }

    public static <T> T toObjWithDefault(String json, Class<T> type) {
        try {
            return toObj(json, type);
        } catch (JsonProcessingException e) {
            logError(json);
            return null;
        }
    }

    private static void logError(String json) {
        log.error("convert json : {} to obj error", json);
    }


}