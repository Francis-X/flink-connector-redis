package com.awifi.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * @author francis
 * @Title: RedisConnectorOptions
 * @Description:
 * @Date 2022-05-06 15:15
 * @since
 */
public class RedisConnectorOptions {

    public static final ConfigOption<String> NODES = ConfigOptions.key("nodes")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional nodes for connect to redis");

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional password for connect to redis");

    public static final ConfigOption<String> COMMAND = ConfigOptions.key("command")
            .stringType()
            .noDefaultValue()
            .withDescription("Optional command for connect to redis");

    public static final ConfigOption<Long> TIMEOUT = ConfigOptions.key("timeout")
            .longType()
            .defaultValue(2000L)
            .withDescription("Optional timeout for connect to redis");
    /**
     * 单机模式,可选字段
     */
    public static final ConfigOption<Integer> DATABASE = ConfigOptions.key("database")
            .intType()
            .defaultValue(0)
            .withDescription("Optional database for connect to redis");

    public static final ConfigOption<Integer> LENGTH = ConfigOptions.key("length")
            .intType()
            .defaultValue(5)
            .withDescription("Optional length for ltrim list");

    public static final ConfigOption<Integer> SINK_PARALLELISM;
    public static final ConfigOption<Integer> SINK_MAX_RETRIES;

    static {
        SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;
        SINK_MAX_RETRIES = ConfigOptions.key("sink.max-retries").intType().defaultValue(3).withDescription("The max retry times if writing records to database failed.");
    }

}
