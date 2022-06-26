package com.awifi.flink;

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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

    private static final String nodes = "192.168.213.169:6379,192.168.213.170:6379,192.168.213.171:6379,192.168.213.172:6379,192.168.213.173:6379,192.168.213.174:6379";

    private static final String password = "****";

    @Test
    public void testRedisHset() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String ddl = "CREATE TABLE redis_hash(\n" +
                "  `key` varchar COMMENT 'key值',\n" +
                "  `name` varchar COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `age` int COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `birthday` TIMESTAMP(3)\n" +
                ") with (\n" +
                "  'connector' = 'redis',\n" +
                "  'nodes' = '" + nodes + "',\n" +
                "  'password' = '" + password + "',\n" +
                "  'command' = 'hset'\n" +
                ")";
        System.out.println(ddl);
        tEnv.executeSql(ddl);

        String sql = "insert into redis_hash select * from (values ('com.awifi.flink-redis-hash', '张三','男',19,NOW()))";
//        for (int i = 0; i < 5; i++) {
//            String sql = "insert into redis_list select * from (values ('com.awifi.flink-redis-ltrim', '张三" + i + "'))";
//            TableResult tableResult = tEnv.executeSql(sql);
//            tableResult.getJobClient().get().getJobExecutionResult().get();
//            System.out.println(sql);
//        }
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testParallel() {
        List<Integer> array = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        List<List<Integer>> partition = Lists.partition(array, 3);
        long t = System.currentTimeMillis();
        partition.parallelStream().forEach(a -> {
            System.out.println(Thread.currentThread().getName() + ":" + a);
            int b = 0;
            for (int i = 0; i < 10000000; i++) {
                b += new Random().nextInt(10);
            }
            System.out.println(Thread.currentThread().getName() + ":" + b);
        });
        System.out.println(System.currentTimeMillis() - t);

    }

}

