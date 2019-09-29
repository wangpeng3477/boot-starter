package com.youqu.redis.routes;

import com.youqu.redis.template.TemplateRoute;
import org.springframework.data.redis.core.RedisTemplate;
import java.util.List;

public class RedisTemplateRoute extends TemplateRoute<RedisTemplate<String, Object>> {
    public RedisTemplateRoute(List<RedisTemplate<String, Object>> list) {
        super(list);
    }
}
