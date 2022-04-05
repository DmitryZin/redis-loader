package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPubSub;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RedisLoader <K, V> {
    private final RedisSet set;
    private final ObjectMapper mapper;
    private final Redis redis;
    private JedisPubSub jedisPubSub;

    private final Class<? extends K> kClass;
    private final Class<? extends V> vClass;

    private Map<K, V> map;

    private boolean isStateReload = false;

    public RedisLoader(Class<? extends K> kClass, Class<? extends V> vClass, String host, int port, RedisSet set, ObjectMapper mapper, boolean isSubscriber) throws JsonProcessingException {
        this.kClass = kClass;
        this.vClass = vClass;
        this.set = set;
        this.mapper = mapper;
        redis = new Redis(host, port);
        reLoad();
        if(isSubscriber){
            subscribe();
        }
    }

    public void reLoad() throws JsonProcessingException {
        if(isStateReload)
            return;
        isStateReload = true;
        try {
            Map<K, V> result = new HashMap<>();
            Map<String, String> keys = redis.getKeyDataSet(set.getDataSetName());
            for (Map.Entry<String, String> entry : keys.entrySet()) {
                result.put(mapper.readValue(entry.getKey(), kClass), mapper.readValue(entry.getValue(), vClass));
            }
            map = result;
        } finally {
            isStateReload = false;
        }
    }

    public Map<K, V> getValues(){
        return new HashMap<>(map);
    }

    public void unSubscribe(){
        redis.unSubscribe(jedisPubSub);
        jedisPubSub = null;
    }

    /**
     * Подписаться на сообщения - создать "слушателя"
     */
    public void subscribe(){
        if(jedisPubSub != null){
            log.info("I'm already subscribe");
            return;
        }
        jedisPubSub = new JedisPubSub(){
            @Override
            public void onMessage(String channel, String message){
                try {
                    reLoad();
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        };
        redis.subscribe(jedisPubSub, set.getChannelName());
    }

}
