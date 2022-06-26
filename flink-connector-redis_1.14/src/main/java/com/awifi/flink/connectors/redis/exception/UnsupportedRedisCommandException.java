package com.awifi.flink.connectors.redis.exception;

import com.awifi.flink.connectors.redis.predefined.RedisSinkCommand;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The type Unsupported redis command exception.
 *
 * @author francis
 * @Title: UnsupportedRedisCommandException
 * @Description:
 * @Date 2022 -05-10 21:01
 * @since
 */
public class UnsupportedRedisCommandException extends RuntimeException {

    private static final String SINK_SUPPORTED_REDIS_COMMAND;

    static {
        SINK_SUPPORTED_REDIS_COMMAND = String.format(" however support command: '%s", Arrays.stream(RedisSinkCommand.values())
                .map(Enum::name)
                .map(String::toLowerCase)
                .collect(Collectors.toList()).toString());
    }

    /**
     * Instantiates a new Unsupported redis command exception.
     *
     * @param message the message
     */
    public UnsupportedRedisCommandException(String message) {
        super(message == null ? SINK_SUPPORTED_REDIS_COMMAND : message + SINK_SUPPORTED_REDIS_COMMAND);
    }
}
