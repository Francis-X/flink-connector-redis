# flink-connector-redis

版本: 1.0 

作者: Francis 

支持flink版本: 1.14.4+



说明：

1. 只支持集群和单机redis
2. 支持sink表和lookup表
3. 目前并未实现 checkpoint 相关功能，有待后续开发。

## Flink SINK SQL 支持命令

### 常用命令

| 命令    | redis数据类型 | 命令格式                                            |
| ------- | ------------- | --------------------------------------------------- |
| set     | String        | set key value                                       |
| hset    | Hash          | hset key field field_value [,key field field_value] |
| lpush   | List          | lpush key value  [,value]                           |
| rpush   | List          | rpush key value  [,value]                           |
| sadd    | Set           | sadd key value  [,value]                            |
| zadd    | Sorted_set    | zadd key score element                              |
| pfadd   | Hyper_log_log | pfadd key element [,element]                        |
| publish | Pubsub        | publish channelName message                         |

### 特定命令

| 命令  | redis数据类型 | redis命令格式           | 备注                                                         |
| ----- | ------------- | ----------------------- | ------------------------------------------------------------ |
| ltrim | List          | ltrim key 0 ${length}-1 | 实际底层先通过lpush key value，增加数据，再调用ltrim key 0 ${length}-1修剪队列 |

## Flink SQL 示例

⚠️注意：所有表对应第一个字段为key值，字段顺序和命令格式一致(Hash除外)，比如sadd命令sink表第二个字段只能为score

### set

```sql
CREATE TABLE redis_set(  
  `key` varchar COMMENT 'key值',
  `value` varchar COMMENT 'value值'
) with (  
  'connector' = 'redis',
  'nodes' = 'ip1:6379,ip2:6379',
  'command' = 'set',  
  'password' = '######'
);
```

### hset

```sql
CREATE TABLE redis_set(  
  `key` varchar COMMENT 'key值',
  `name` varchar COMMENT '字段名对应Hash field,值对应field_value',
  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',
  `age` int COMMENT '字段名对应Hash field,值对应field_value'
) with (  
  'connector' = 'redis',
  'nodes' = 'ip1:6379,ip2:6379',
  'command' = 'hset',  
  'password' = '######'
);
```

### zadd

```sql
CREATE TABLE redis_set(  
  `key` varchar COMMENT 'key值',
  `score` double COMMENT '分值',
  `value` varchar COMMENT 'element值'
) with (  
  'connector' = 'redis',
  'nodes' = 'ip1:6379,ip2:6379',
  'command' = 'zadd',  
  'password' = '######'
);
```

### ltrim

```sql
CREATE TABLE redis_list(  
  `key` varchar COMMENT 'key值',
  `value` varchar COMMENT 'value值'
) with (  
  'connector' = 'redis',
  'nodes' = 'ip1:6379,ip2:6379',
  'command' = 'ltrim',  
  'password' = '######'
);
```

## 连接器参数

| 参数                  | 是否必选 | 默认值  | 数据类型 | 描述                                                         |
| --------------------- | -------- | ------- | -------- | ------------------------------------------------------------ |
| connector             | 必选     | (无)    | String   | 指定使用的连接器，redis连接器使用'redis'                     |
| nodes                 | 必选     | (无)    | String   | 逗号分隔的 redis node 列表                                   |
| command               | 必选     | (无)    | String   | 支持的命令                                                   |
| password              | 可选     | (无)    | String   | 如果redis启用密码                                            |
| timeout               | 可选     | 2000    | Long     | redis连接超时时间                                            |
| ttl                   | 可选     | 604800L | Long     | redis过期时间                                                |
| length                | 可选     | 5       | Integer  | ltrim命令下固定长度大小                                      |
| database              | 可选     | 0       | Integer  | 单机模式下连接的数据库，默认数据库0                          |
| sink.max-retries      | 可选     | 3       | Integer  | 集群模式下最大重定向次数                                     |
| sink.parallelism      | 可选     | 1       | Integer  | 定义 redis sink 算子的并行度。默认情况下，并行度由框架定义为与上游串联的算子相同 |
| lookup.max-retries    | 可选     | 3       | Integer  | 查询redis失败的最大重试时间。                                |
| lookup.cache.max-rows | 可选     | (无)    | Integer  | lookup cache 的最大行数，若超过该值，则最老的行记录将会过期。 默认情况下，lookup cache 是未开启的。 |
| lookup.cache.ttl      | 可选     | (none)  | Duration | lookup cache 中每一行记录的最大存活时间，若超过该时间，则最老的行记录将会过期。 默认情况下，lookup cache 是未开启的。 |

## Flink lookup source

### 支持的命令

| 命令    | redis数据类型 | 命令格式    |
| ------- | ------------- | ----------- |
| get     | String        | get key     |
| hgetall | Hash          | hgetall key |

### 案例

```
CREATE TABLE file_table (
  id INT,
  `data1` STRING,
  `key` varchar,
  data2 STRING,
  proctime as proctime()
) WITH (
  'connector' = 'filesystem',           -- required: 连接器类型filesystem
  'path' = 'test.txt',  -- required: 文件目录
  'format' = 'csv',                     -- required: 格式
  'csv.field-delimiter' = ','
);

CREATE TABLE redis_lookup_hash(
  `key` varchar COMMENT 'key值',
  `name` varchar COMMENT '字段名对应Hash field,值对应field_value',
  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',
  `age` int COMMENT '字段名对应Hash field,值对应field_value',
  `birthday` TIMESTAMP(3)
) with (
  'connector' = 'redis',
  'nodes' = '#####',
  'password' = '#####',
  'command' = 'hgetall'
);

create table print_sink_table(
  id int,
  data1 varchar,
  `key` varchar,
  data2 varchar,
  name varchar,
  `sex` varchar COMMENT '字段名对应Hash field,值对应field_value',
  `age` int COMMENT '字段名对应Hash field,值对应field_value',
  `birthday` TIMESTAMP(3)
) with (
    'connector' = 'print'
);

insert into print_sink_table
select
a.id,
a.data1,
a.`key`,
a.data2,
b.name
b.`sex` ,
b.`age`,
b.`birthday`
from file_table as a
left join redis_lookup_hash for system_time as of a.proctime as b on a.`key` = b.`key`

#输出结果
```

## Lookup Cache

默认情况下，lookup cache 是未启用的，你可以设置 `lookup.cache.max-rows` and `lookup.cache.ttl` 参数来启用。

## 附录
