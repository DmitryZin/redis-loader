package ru.spbe.redisrefresher;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import ru.spbe.redisloader.RedisLoader;
import ru.spbe.redisstarter.IRedisSubscriber;

import java.util.List;

@RequiredArgsConstructor
public class RedisRefresher<T> implements IRedisSubscriber {
    private final RedisLoader<T> loader;

    @Getter
    @Setter
    private List<T> list;


    @Override
    public void onMessage(String message) {
        if (list == null)
            return;
        list.clear();
        try {
            list.addAll(loader.getListValues());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
