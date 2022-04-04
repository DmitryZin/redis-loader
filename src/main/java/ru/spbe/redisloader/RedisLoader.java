package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPubSub;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

@Slf4j
public abstract class RedisLoader {
    final RedisSet set;
    final ObjectMapper mapper;

    JedisPubSub jedisPubSub;
    Redis redis;

    protected RedisLoader(String host, int port, RedisSet set, ObjectMapper mapper, boolean isSubscriber) throws JsonProcessingException {
        this.set = set;
        this.mapper = mapper;
        redis = new Redis(host, port);
        if(isSubscriber){
            subscribe();
        }
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

    public abstract void reLoad() throws JsonProcessingException;
}
