package com.awifi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * @author francis
 * @Title: RedisLookupTest
 * @Description:
 * @Date 2022-06-16 11:25
 * @since
 */
public class RedisLookupTableTest {

    private static final String nodes = "****";

    private static final String password = "*****";

    @Test
    public void testRedisGet() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        final File file = new File("file/test.txt");

        String path = file.getAbsolutePath();
        String file_ddl = "CREATE TABLE file_table (\n" +
                "  id INT,\n" +
                "  `data1` STRING,\n" +
                "  `key` varchar,\n" +
                "  data2 STRING,\n" +
                "  proctime as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- required: 连接器类型filesystem\n" +
                "  'path' = '" + path + "',  -- required: 文件目录\n" +
                "  'format' = 'csv',                     -- required: 格式\n" +
                "  'csv.field-delimiter' = ','\n" +
                ")";
        tEnv.executeSql(file_ddl);

        String redis_ddl = "CREATE TABLE redis_lookup_string(\n" +
                "  `key` varchar,\n" +
                "  `value1` String,\n" +
                "  process_time as proctime()\n" +
                ") with (\n" +
                "  'connector' = 'redis',\n" +
                "  'nodes' = '" + nodes + "',\n" +
                "  'password' = '" + password + "',\n" +
                "  'command' = 'get'\n" +
                ")";
        tEnv.executeSql(redis_ddl);

        String print_ddl = "create table print_sink_table(\n" +
                "  id int,\n" +
                "  data1 varchar,\n" +
                "  `key` varchar,\n" +
                "  data2 varchar,\n" +
                "  redis_value varchar\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(print_ddl);

        String sql = "insert into print_sink_table\n" +
                "select\n" +
                "a.id,\n" +
                "a.data1,\n" +
                "a.`key`,\n" +
                "a.data2,\n" +
                "b.value1\n" +
                "from file_table as a\n" +
                "left join redis_lookup_string for system_time as of a.proctime as b on a.`key` = b.`key`";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);
    }

    @Test
    public void testRedisHgetall() throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        final File file = new File("file/test01.txt");

        String path = file.getAbsolutePath();
        String file_ddl = "CREATE TABLE file_table (\n" +
                "  id INT,\n" +
                "  `data1` STRING,\n" +
                "  `key` varchar,\n" +
                "  data2 STRING,\n" +
                "  proctime as proctime()\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',           -- required: 连接器类型filesystem\n" +
                "  'path' = '" + path + "',  -- required: 文件目录\n" +
                "  'format' = 'csv',                     -- required: 格式\n" +
                "  'csv.field-delimiter' = ','\n" +
                ")";
        tEnv.executeSql(file_ddl);

        String redis_ddl = "CREATE TABLE redis_lookup_hash(\n" +
                "  `key` varchar COMMENT 'key值',\n" +
                "  `name` varchar COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `age` int COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `birthday` TIMESTAMP(3)\n" +
                ") with (\n" +
                "  'connector' = 'redis',\n" +
                "  'nodes' = '" + nodes + "',\n" +
                "  'password' = '" + password + "',\n" +
                "  'command' = 'hgetall'\n" +
                ")";
        tEnv.executeSql(redis_ddl);

        String print_ddl = "create table print_sink_table(\n" +
                "  id int,\n" +
                "  data1 varchar,\n" +
                "  `key` varchar,\n" +
                "  data2 varchar,\n" +
                "  name varchar,\n" +
                "  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `age` int COMMENT '字段名对应Hash field,值对应field_value',\n" +
                "  `birthday` TIMESTAMP(3)\n" +
                ") with (\n" +
                "    'connector' = 'print'\n" +
                ")";
        tEnv.executeSql(print_ddl);

        String sql = "insert into print_sink_table\n" +
                "select\n" +
                "a.id,\n" +
                "a.data1,\n" +
                "a.`key`,\n" +
                "a.data2,\n" +
                "b.name,\n" +
                "" +
                "b.`sex` ,\n" +
                "b.`age`,\n" +
                "b.`birthday`\n" +
                "from file_table as a\n" +
                "left join redis_lookup_hash for system_time as of a.proctime as b on a.`key` = b.`key`";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get().getJobExecutionResult().get();
        System.out.println(sql);

    }

}
