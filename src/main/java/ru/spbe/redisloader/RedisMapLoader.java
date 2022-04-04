package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.spbe.redisstarter.RedisSet;

import java.util.HashMap;
import java.util.Map;

public class RedisMapLoader<K, V> extends RedisLoader{

    private final Class<? extends K> kClass;
    private final Class<? extends V> vClass;

    private Map<K, V> map;

    public RedisMapLoader(Class<? extends K> kClass, Class<? extends V> vClass, String host, int port, RedisSet set, ObjectMapper mapper, boolean isSubscriber) throws JsonProcessingException {
        super(host, port, set, mapper, isSubscriber);
        this.kClass = kClass;
        this.vClass = vClass;
        reLoad();
    }

    public Map<K, V> getValues(){
        return new HashMap<>(map);
    }

    @Override
    public void reLoad() throws JsonProcessingException {
        Map<K, V> result = new HashMap<>();
        Map<String, String> keys = redis.getKeyDataSet(set.getDataSetName());
        for(Map.Entry<String, String> entry : keys.entrySet()){
            result.put(mapper.readValue(entry.getKey(), kClass), mapper.readValue(entry.getValue(), vClass));
        }
        map = result;
    }
}
