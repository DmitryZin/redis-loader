package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.IRedisSet;
import ru.spbe.redisstarter.Redis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class RedisLoader<T> {
    private final Redis redis;
    private final IRedisSet set;
    private final ObjectMapper mapper;

    public Set<T> getValues(Class<T> tClass) throws JsonProcessingException {
        java.util.Set<T> result = new HashSet<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        return result;
    }

    public List<T> getListValues(Class<T> tClass) throws JsonProcessingException {
        List<T> result = new ArrayList<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        return result;
    }
}
