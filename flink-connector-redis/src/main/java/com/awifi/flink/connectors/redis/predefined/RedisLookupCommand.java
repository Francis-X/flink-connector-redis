package com.awifi.flink.connectors.redis.predefined;

/**
 * The enum Redis lookup command.
 *
 * @author francis
 * @Title: RedisLookupCommand
 * @Description:
 * @Date 2022 -06-15 19:14
 * @since
 */
public enum RedisLookupCommand {

    //    HGET(RedisDataType.HASH),


    /**
     * Hgetall redis lookup command.
     */
    HGETALL(RedisDataType.HASH),


    /**
     * Get redis lookup command.
     */
    GET(RedisDataType.SET);


    private final RedisDataType redisDataType;


    RedisLookupCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }


    /**
     * Gets redis data type.
     *
     * @return the redis data type
     */
    public RedisDataType getRedisDataType() {
        return redisDataType;
    }
}
