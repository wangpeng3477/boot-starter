package com.wp.redis.template;

import redis.clients.util.Hashing;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class TemplateRoute<T> {

    private TreeMap<Long, T> nodes = new TreeMap<Long, T>();
    private final Hashing algo = Hashing.MURMUR_HASH;

    public TemplateRoute(List<T> list) {
        //保证 List<ChapterTemplate> 顺序 ,减少缓存的迁移
        for (int i = 0; i != list.size(); ++i) {
            final T t = list.get(i);
            for (int n = 0; n < 160; n++) {
                nodes.put(this.algo.hash("SHARD-" + i + "-NODE-" + n), t);
            }
        }
    }

    public T getTemplate(String key) {
        SortedMap<Long, T> tail = nodes.tailMap(algo.hash(key));
        if (tail.isEmpty()) {
            return nodes.get(nodes.firstKey());
        }
        return tail.get(tail.firstKey());
    }
}