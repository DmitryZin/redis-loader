package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.spbe.redisstarter.RedisSet;

import java.util.HashMap;
import java.util.Map;

public class RedisMapLoader<K, T> extends RedisLoader{

    private final Class<? extends T> tClass;
    private final Class<? extends K> kClass;

    private Map<K, T> map;

    public RedisMapLoader(Class<? extends K> kClass, Class<? extends T> tClass, String host, int port, RedisSet set, ObjectMapper mapper, boolean isSubscriber) throws JsonProcessingException {
        super(host, port, set, mapper, isSubscriber);
        this.kClass = kClass;
        this.tClass = tClass;
        reLoad();
    }

    public Map<K, T> getValues(){
        return new HashMap<>(map);
    }

    @Override
    public void reLoad() throws JsonProcessingException {
        Map<K, T> result = new HashMap<>();
        Map<String, String> keys = redis.getKeyDataSet(set.getDataSetName());
        for(Map.Entry<String, String> entry : keys.entrySet()){
            result.put(mapper.readValue(entry.getKey(), kClass), mapper.readValue(entry.getValue(), tClass));
        }
        map = result;
    }
}
