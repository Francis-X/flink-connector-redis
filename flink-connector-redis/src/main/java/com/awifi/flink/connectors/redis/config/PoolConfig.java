package com.awifi.flink.connectors.redis.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Serializable;

/**
 * @author francis
 * @Title: PoolConfig
 * @Description:
 * @Date 2022-06-18 19:38
 * @since
 */
public class PoolConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final int maxIdle;
    protected final int minIdle;
    protected final int maxTotal;

    public PoolConfig(int maxIdle, int minIdle, int maxTotal) {
        this.maxIdle = maxIdle;
        this.minIdle = minIdle;
        this.maxTotal = maxTotal;
    }

    protected PoolConfig(Builder builder) {
        this(builder.maxIdle, builder.minIdle, builder.maxTotal);
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public static class Builder {
        private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
        private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
        private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;

        /**
         * Sets value for the {@code maxTotal} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxTotal maxTotal the maximum number of objects that can be allocated by the pool, default value is 8
         * @return Builder itself
         */
        public Builder maxTotal(int maxTotal) {
            this.maxTotal = maxTotal;
            return this;
        }

        /**
         * Sets value for the {@code maxIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param maxIdle the cap on the number of "idle" instances in the pool, default value is 8
         * @return Builder itself
         */
        public Builder maxIdle(int maxIdle) {
            this.maxIdle = maxIdle;
            return this;
        }

        /**
         * Sets value for the {@code minIdle} configuration attribute
         * for pools to be created with this configuration instance.
         *
         * @param minIdle the minimum number of idle objects to maintain in the pool, default value is 0
         * @return Builder itself
         */

        public Builder minIdle(int minIdle) {
            this.minIdle = minIdle;
            return this;
        }

        public PoolConfig build() {
            return new PoolConfig(this);
        }
    }

}
