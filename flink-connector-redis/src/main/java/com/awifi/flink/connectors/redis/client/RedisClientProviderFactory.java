package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.config.FlinkLettuceRedisConfig;

import java.util.Objects;

/**
 * @author francis
 * @Title: RedisClientProviderFactory
 * @Description:
 * @Date 2022-06-15 14:33
 * @since
 */
public class RedisClientProviderFactory {

    public static RedisClientProvider redisClientProvider(FlinkLettuceRedisConfig flinkLettuceRedisConfig) {
        Objects.requireNonNull(flinkLettuceRedisConfig, "The redis connection configuration should be null ");
        if (flinkLettuceRedisConfig.getNodes().size() == 1) {
            return new SingletonRedisClientProvider(flinkLettuceRedisConfig);
        }
        return new ClusterRedisClientProvider(flinkLettuceRedisConfig);
    }

}
