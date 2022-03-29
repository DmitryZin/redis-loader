package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
public class RedisLoader<T> {
    private final Class<? extends T> tClass;
    private final Redis redis;
    private final RedisSet set;
    private final ObjectMapper mapper;

    public Set<T> getValues() throws JsonProcessingException {
        java.util.Set<T> result = new HashSet<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        return result;
    }

    public List<T> getListValues() throws JsonProcessingException {
        List<T> result = new ArrayList<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        return result;
    }
}
