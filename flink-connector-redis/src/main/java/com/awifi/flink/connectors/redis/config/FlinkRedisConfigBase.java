package com.awifi.flink.connectors.redis.config;

import java.io.Serializable;

/**
 * @author francis
 * @Title: FlinkRedisConfigBase
 * @Description:
 * @Date 2022-05-06 17:27
 * @since
 */
public abstract class FlinkRedisConfigBase implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final long connectionTimeout;
    protected final String password;
    protected final int database;

    protected FlinkRedisConfigBase(long connectionTimeout, String password, int database) {
        this.connectionTimeout = connectionTimeout;
        this.password = password;
        this.database = database;
    }


    public long getConnectionTimeout() {
        return connectionTimeout;
    }


    public int getDatabase() {
        return database;
    }
}
