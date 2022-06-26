package com.awifi.flink.connectors.redis.options;

import java.io.Serializable;

/**
 * @author francis
 * @Title: RedisLookupOption
 * @Description: 维表操作选项配置
 * @Date 2022-06-09 09:51
 * @since
 */
public class RedisLookupOptions implements Serializable {

    private static final long serialVersionUID = -1L;
    /**
     * 缓存最大行
     */
    private final long cacheMaxSize;
    /**
     * 缓存过期时间
     */
    private final long cacheExpireMs;

    /**
     * 查询数据库失败的最大重试次数
     */
    private final int maxRetryTimes;


    public RedisLookupOptions(long cacheMaxSize, long cacheExpireMs, int maxRetryTimes) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheExpireMs = -1L;
        private int maxRetryTimes = 3;

        private Builder() {
        }

        public Builder cacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder cacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public Builder maxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }

        public RedisLookupOptions build() {
            return new RedisLookupOptions(this.cacheMaxSize, this.cacheExpireMs, this.maxRetryTimes);
        }
    }
}
