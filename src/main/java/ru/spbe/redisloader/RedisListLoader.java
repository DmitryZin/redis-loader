package ru.spbe.redisloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.spbe.redisstarter.Redis;
import ru.spbe.redisstarter.RedisSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *  Класс {@link RedisListLoader} реализует чтение данных из БД Redis
 *
 *  * Требует  {@link Redis} - подключение к БД
 *  * Требует  {@link RedisSet} - описание хранения данных
 *  * Требует  {@link ObjectMapper} - для сериализации, чтоб не создавать свой экземпляр для каждого класса
 *  * также в конструкторе требуется указать класс для десиарилации
 */
@Slf4j
public class RedisListLoader<T>  extends RedisLoader{
    private final Class<? extends T> tClass;
    private List<T> list;

    public RedisListLoader(Class<? extends T> tClass, String host, int port, RedisSet set, ObjectMapper mapper, boolean isSubscriber) throws JsonProcessingException {
        super(host, port, set,mapper, isSubscriber);
        this.tClass = tClass;
        reLoad();
    }

    public List<T> getValues(){
        return new ArrayList<>(list);
    }

    @Override
    public void reLoad() throws JsonProcessingException {
        List<T> result = new ArrayList<>();
        Set<String> setValues = redis.getDataSet(set.getDataSetName());
        for(String value : setValues){
            result.add(mapper.readValue(value, tClass));
        }
        list = result;
    }
}
