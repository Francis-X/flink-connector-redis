package com.awifi.flink.connectors.redis.sink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * @author francis
 * @Title: RedisSinkProcessFunction
 * @Description:
 * @Date 2022-05-09 14:46
 * @since
 */
public interface RedisSinkProcessFunction<T, C> extends Serializable, Function {
    default void open() throws Exception {
    }

    default void close() throws Exception {
    }

    void process(T var1, C var2, RuntimeContext var3) throws Exception;
}
