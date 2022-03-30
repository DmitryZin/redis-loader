package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPubSub;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *  Класс {@link RedisLoader} реализует чтение данных из БД Redis
 *
 *  * Требует  {@link Redis} - подключение к БД
 *  * Требует  {@link RedisSet} - описание хранения данных
 *  * Требует  {@link ObjectMapper} - для сериализации, чтоб не создавать свой экземпляр для каждого класса
 *  * также в конструкторе требуется указать класс для десиарилации
 */
@Slf4j
@RequiredArgsConstructor
public class RedisLoader<T> {
    private final Class<? extends T> tClass;
    private final Redis redis;
    private final RedisSet set;
    private final ObjectMapper mapper;

    private JedisPubSub jedisPubSub;
    private List<T> list;

    public List<T> getListValues() throws JsonProcessingException {
        List<T> result = new ArrayList<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        return result;
    }

    public void unSubscribe(){
        redis.unSubscribe(jedisPubSub);
        jedisPubSub = null;
    }

    public void reLoad() throws JsonProcessingException {
        if(list == null)
            return;
        list.clear();
        list.addAll(getListValues());
    }
    /**
     * Подписаться на сообщения - создать "слушателя"
     */
    public void subscribe(List<T> list){
        this.list = list;
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
