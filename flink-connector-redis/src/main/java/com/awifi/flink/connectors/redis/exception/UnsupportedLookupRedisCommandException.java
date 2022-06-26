package com.awifi.flink.connectors.redis.exception;

/**
 * @author francis
 * @Title: UnsupportedLookupRedisException
 * @Description:
 * @Date 2022-06-15 18:42
 * @since
 */
public class UnsupportedLookupRedisCommandException extends RuntimeException {

    public UnsupportedLookupRedisCommandException(String message) {
        super(message);
    }
}
