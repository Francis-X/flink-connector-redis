package com.awifi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

/**
 * @author francis
 * @Title: RedisTableTest
 * @Description:
 * @Date 2022-05-10 10:16
 * @since
 */
public class RedisTableTest {
    private static final Logger LOG = LoggerFactory.getLogger(RedisTableTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String nodes = args[0];
        String password = args[1];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);


        String ddl = "CREATE TABLE redis_list(\n" +
                "  `key` varchar,\n" +
                "  `value1` varchar\n" +
                ") with (\n" +
                "  'connector' = 'redis',\n" +
                "  'nodes' = '" + nodes + "',\n" +
                "  'password' = '" + password + "',\n" +
                "  'command' = 'ltrim'\n" +
                ")";
        System.out.println(ddl);
        tEnv.executeSql(ddl);
        String sql = "insert into redis_list select * from (values ('com.awifi.flink-redis-ltrim', '李四'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);

//
//        Optional<Integer> optional = Optional.empty();
//
//        System.out.println(optional.orElse(RedisConnectorOptions.SINK_PARALLELISM.defaultValue()));
    }
}

