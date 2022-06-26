package com.awifi.flink.connectors.redis.exception;

import com.awifi.flink.connectors.redis.mapper.RedisCommand;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author francis
 * @Title: UnsupportedRedisCommandException
 * @Description:
 * @Date 2022-05-10 21:01
 * @since
 */
public class UnsupportedRedisCommandException extends RuntimeException {

    private static final String SUPPORTED_REDIS_COMMAND;

    static {
        SUPPORTED_REDIS_COMMAND = String.format(" however support command: '%s", Arrays.stream(RedisCommand.values())
                .map(Enum::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList()).toString());
    }

    public UnsupportedRedisCommandException(String message) {
        super(message == null ? SUPPORTED_REDIS_COMMAND : message + SUPPORTED_REDIS_COMMAND);
    }
}
