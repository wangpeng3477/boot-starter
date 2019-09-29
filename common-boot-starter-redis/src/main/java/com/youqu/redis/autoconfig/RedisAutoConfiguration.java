package com.youqu.redis.autoconfig;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youqu.redis.facade.RedisFacade;
import com.youqu.redis.properties.RedisConfigProperties;
import com.youqu.redis.properties.RedissonConfigProperties;
import com.youqu.redis.routes.RedisTemplateRoute;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

@EnableConfigurationProperties(value = {RedisConfigProperties.class, RedissonConfigProperties.class})
@Configuration
public class RedisAutoConfiguration {
    @Autowired
    private RedisConfigProperties redisConfigProperties;
    @Autowired
    private RedissonConfigProperties redissonConfigProperties;

    @Bean
    public JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(redisConfigProperties.getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisConfigProperties.getMaxIdle());
        jedisPoolConfig.setMinIdle(redisConfigProperties.getMinIdle());
        jedisPoolConfig.setMaxWaitMillis(redisConfigProperties.getMaxWaitMillis());
        jedisPoolConfig.setMinEvictableIdleTimeMillis(redisConfigProperties.getMinEvictableIdleTimeMillis());
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(redisConfigProperties.getTimeBetweenEvictionRunsMillis());
        return jedisPoolConfig;
    }

    @Bean
    public RedisTemplateRoute redisTemplateRoute(JedisPoolConfig jedisPoolConfig) {
        List<RedisTemplate<String, Object>> redisTemplateList = new ArrayList<>();
        for (String hostName : redisConfigProperties.getHostList()) {
            JedisConnectionFactory jedisConnectionFactory = jedisConnectionFactorys(jedisPoolConfig, hostName);
            RedisTemplate<String, Object> redisTemplate = this.getRedisTemplate(jedisConnectionFactory);
            redisTemplateList.add(redisTemplate);
        }
        RedisTemplateRoute route = new RedisTemplateRoute(redisTemplateList);
        return route;
    }

    private JedisConnectionFactory jedisConnectionFactorys(JedisPoolConfig jedisPoolConfig, String hostName) {
        JedisConnectionFactory factory = new JedisConnectionFactory();
        factory.setPoolConfig(jedisPoolConfig);
        factory.setHostName(hostName);
        factory.setPort(redisConfigProperties.getPort());
        factory.setPassword(redisConfigProperties.getPassword());
        factory.setTimeout(redisConfigProperties.getTimeout());
        factory.setUsePool(redisConfigProperties.isUsePool());
        factory.setDatabase(redisConfigProperties.getDatabase());
        return factory;
    }

    private RedisTemplate<String, Object> getRedisTemplate(JedisConnectionFactory redisConnectionFactory) {
        RedisSerializer<String> stringSerializer = new StringRedisSerializer();
        Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<>(Object.class);
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jackson2JsonRedisSerializer.setObjectMapper(om);
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(jackson2JsonRedisSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(jackson2JsonRedisSerializer);
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public Config config() {
        Config config = new Config();
        config.useSingleServer().setAddress(redissonConfigProperties.getHostName() + ":" + redissonConfigProperties.getPort()).setDatabase(redissonConfigProperties.getDatabase()).setConnectionPoolSize(redissonConfigProperties.getConnectPoolSize()).setPassword(redissonConfigProperties.getPassword());
        return config;
    }

    @Bean
    public RedissonClient redissonClient(Config config) {
        return Redisson.create(config);
    }

    @Bean
    public RedisFacade redisFacade(RedisTemplateRoute redisTemplateRoute, RedissonClient redissonClient) {
        return new RedisFacade(redisConfigProperties, redisTemplateRoute, redissonClient);
    }
}
