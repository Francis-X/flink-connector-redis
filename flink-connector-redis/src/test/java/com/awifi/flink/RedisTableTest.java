package com.awifi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
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

    private static final String nodes = "192.168.213.169:6379,192.168.213.170:6379,192.168.213.171:6379,192.168.213.172:6379,192.168.213.173:6379,192.168.213.174:6379";

    private static final String password = "*****";

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


//    public static void main(String[] args) {
//////        String str = "^((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}(:[1-9][0-9]*)(,((?!-)[A-Za-z0-9-]{1,63}(?<!-)\\.)+[A-Za-z]{2,6}(:[1-9][0-9]*))*$";
//        String ip =  "^(((25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|[0-1]\\d{2}|[1-9]?\\d)(:([0-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{4}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))($|(?!,$),))+$";
//        Pattern compile = Pattern.compile(ip);
//        Matcher matcher = compile.matcher("192.168.10.1:9090,192.168.10.1:9090");
//        if (matcher.matches()) {
//            System.out.println(1111111111);
//        }
////        System.out.println("------------");
//
//    }
}

