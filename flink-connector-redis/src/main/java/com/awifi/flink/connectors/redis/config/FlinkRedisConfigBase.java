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

    protected final int maxTotal;
    protected final int maxIdle;
    protected final int minIdle;

    protected final long connectionTimeout;
    protected final String password;
    protected final int database;

    protected FlinkRedisConfigBase(int maxTotal, int maxIdle, int minIdle, long connectionTimeout, String password, int database) {
        this.maxTotal = maxTotal;
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.connectionTimeout = connectionTimeout;
        this.password = password;
        this.database = database;
    }


    public int getMaxTotal() {
        return maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }


    public int getDatabase() {
        return database;
    }
}
