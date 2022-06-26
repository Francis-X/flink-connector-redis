package com.awifi.flink.connectors.redis.client;

import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

import java.io.Serializable;

/**
 * @author francis
 * @Title: RedisClientProvider
 * @Description:
 * @Date 2022-06-15 09:06
 * @since
 */
public interface RedisClientProvider extends AutoCloseable, Serializable {

    /**
     * 获取redis客户端操作命令
     *
     * @return
     * @throws Exception
     */
    RedisClusterCommands getRedisClientCommands() throws Exception;



}
