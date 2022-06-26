package com.awifi.flink.connectors.redis.client;

import com.awifi.flink.connectors.redis.exception.UnsupportedRedisCommandException;
import com.awifi.flink.connectors.redis.predefined.RedisSinkCommand;
import com.awifi.flink.connectors.redis.predefined.RedisDataType;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.io.Serializable;
import java.util.Map;

/**
 * @author francis
 * @Title: RedisCommandsExecutor
 * @Description:
 * @Date 2022-06-15 11:52
 * @since
 */
public class RedisCommandsExecutor<K, V> {


    private RedisClusterCommands<K, V> commands;

    private RedisSinkCommand redisSinkCommand;

    private RedisCommandsExecutor(RedisClusterCommands<K, V> commands, RedisSinkCommand redisSinkCommand) {
        this.commands = commands;
        this.redisSinkCommand = redisSinkCommand;
    }

    public static RedisCommandsExecutor commandsExecutor(RedisClusterCommands<Object, Object> commands, RedisSinkCommand redisSinkCommand) {
        return new RedisCommandsExecutor(commands, redisSinkCommand);
    }

    private boolean expire(K key, long ttl) {
        if (ttl != -1) {
            return commands.expire(key, ttl);
        }
        return false;
    }

    private Object[] validateDouble(Object[] scoredValue) {
        int length = scoredValue.length;
        if (length % 2 != 0) {
            String msg = String.format("zadd command score and value come in pairs");
            throw new IllegalArgumentException(msg);
        }

        for (int i = 0; i < length; i += 2) {
            try {
                scoredValue[i] = Double.parseDouble(String.valueOf(scoredValue[i]));
            } catch (Exception e) {
                String msg = String.format("zadd command score must be Double DataType but '%s' is not", scoredValue[i]);
                throw new IllegalArgumentException(msg);
            }
        }
        return scoredValue;
    }


    public final RedisCommands redisCommands() {
        RedisDataType redisDataType = redisSinkCommand.getRedisDataType();
        switch (redisDataType) {
            case STRING:
                return redisStringCommands();
            case HASH:
                return redisHashCommands();
            case LIST:
                return redisListCommands();
            case SET:
                return redisSetCommands();
            case SORTED_SET:
                return redisSortedSetCommands();
            case PUBSUB:
                return redisPubsubCommands();
            case HYPER_LOG_LOG:
                return redisHLLCommands();
            default:
                return (ttl, key, value) -> {
                    throw new UnsupportedRedisCommandException(null);
                };
        }
    }

    private RedisCommands<K, V[]> redisHLLCommands() {
        return (ttl, key, value) -> {
            commands.pfadd(key, value);
            expire(key, ttl);
        };
    }

    private RedisCommands<K, V[]> redisPubsubCommands() {
//        if (!redisSinkCommand.equals(RedisSinkCommand.PUBLISH)) {
//            throw new UnsupportedRedisCommandException(null);
//        }
        return (ttl, key, value) -> {
            commands.publish(key, value[0]);
        };
    }


    private RedisCommands<K, Object[]> redisSortedSetCommands() {
        return (ttl, key, value) -> {
            Object[] scoredValues = validateDouble(value);
            commands.zadd(key, scoredValues);
        };
    }

    private RedisCommands<K, V[]> redisSetCommands() {
        return (ttl, key, value) -> {
            commands.sadd(key, value);
            expire(key, ttl);
        };
    }

    private RedisCommands<K, V[]> redisListCommands() {
        switch (redisSinkCommand) {
            case LPUSH:
                return (ttl, key, value) -> {
                    commands.lpush(key, value);
                    expire(key, ttl);
                };
            case RPUSH:
                return (ttl, key, value) -> {
                    commands.rpush(key, value);
                    expire(key, ttl);
                };
            case LTRIM:
                return (ttl, key, value) -> {
                    V v = value[value.length - 1];
                    commands.lpush(key, value[0]);
                    commands.ltrim(key, 0, Long.parseLong(String.valueOf(v)) - 1);
                    expire(key, ttl);
                };
            default:
                return (ttl, key, value) -> {
                    throw new UnsupportedRedisCommandException(null);
                };
        }
    }


    /**
     * @return
     */
    private RedisCommands<K, Map<K, V>> redisHashCommands() {
        return (ttl, key, value) -> {
            commands.hmset(key, value);
            expire(key, ttl);
        };
    }

    /**
     * @return
     */
    private RedisCommands<K, V[]> redisStringCommands() {
        return (ttl, key, value) -> commands.setex(key, ttl, value[0]);
    }

    @FunctionalInterface
    public interface RedisCommands<K, V> extends Serializable {
        /**
         * 执行命令
         *
         * @param ttl
         * @param key
         * @param value
         * @throws Exception
         */
        void execute(long ttl, K key, V value) throws Exception;
    }

}
