[TOC]
# 1、服务器信息
## 新集群
### 生产
|    系统    |  作用 |                    ip或网址                    |    用户   |       密码       |    备注   |
|------------|-------|------------------------------------------------|-----------|------------------|-----------|
| linux      | ftp   | 10.90.0.5                                      |           |                  |           |
| node47     | linux | 10.80.1.47                                     | root      | CQBP53G(Lv82     |           |
| node148    | linux | 10.80.1.148                                    | root      | CQBP53G(Lv82     |           |
| node172    | linux | 10.80.1.172                                    | root      | CQBP53G(Lv82     |           |
| System     | linux | 10.83.0.10                                     | root      | Ig793&0ni1lFepYW | 系统      |
| SYSMySQL   | mysql | 10.80.16.9                                     | root      | !mAkJTMI%lH5ONDw | 核心 旧   |
| SYSMySQL   | mysql | 10.80.16.10                                    | root      | LZWkT2lxze6x%1V( | 核心 新   |
| SYSMySQL   | mysql | 10.80.16.25                                    | root      | LZWkT2lxze6x%1V( | 核心 回放 |
| SYSMySQL   | mysql | 10.80.16.87                                    | root      | wH7Emvsrg&V5     | 催收      |
| CMDB       | mysql | 10.80.16.75                                    | bgp_admin | U3$AHfp*a8M&     | CM        |
| cm         | web   | http://10.80.1.47:7180/cmf/home                | admin     | admin            |           |
| hue        | web   | http://10.80.1.47:8889/hue/editor/?type=impala | admin     | dFGYXpxifv       |           |
| streamsets | web   | http://10.80.1.172:18630/                      | admin     | admin            |           |

### 测试
|    系统    |  作用 |                    ip或网址                     |    用户   |       密码       |   备注  |
|------------|-------|-------------------------------------------------|-----------|------------------|---------|
| linux      | ftp   | 10.83.0.32                                      | it-dev    | 058417gv         |         |
| node47     | linux | 10.83.0.47                                      | root      | (Ob!)Y#G3Anf     |         |
| node123    | linux | 10.83.0.123                                     | root      | (Ob!)Y#G3Anf     |         |
| node129    | linux | 10.83.0.129                                     | root      | (Ob!)Y#G3Anf     |         |
| System     | linux | 10.83.0.10                                      | root      | tf$Ke^HB5lm&     |         |
| SYSMySQL   | mysql | 10.83.16.43                                     | root      | zU!ykpx3EG)$$1e6 | 抵消 $$ |
| mariaDB    | mysql | 10.83.16.32                                     | bgp_admin | 3Mt%JjE#WJIt     |         |
| cm         | web   | http://10.83.0.47:7180/cmf/home                 | admin     | admin            |         |
| hue        | web   | http://10.83.0.123:8889/hue/editor/?type=impala | admin     | admin            |         |
| streamsets | web   | http://10.83.0.129:18630                        | admin     | admin            |         |

## 旧集群
### 生产
|    hostname   |  ip或网址  | 用户 |     密码    | 备注 |
|---------------|------------|------|-------------|------|
| BSPRD-Hadoop1 | 10.80.0.20 | root | Xfx2018@)!* |      |
| BSPRD-Hadoop2 | 10.80.0.23 | root | Xfx2018@)!* |      |
| BSPRD-Hadoop3 | 10.80.0.29 | root | Xfx2018@)!* |      |
| 数据库mysql   | 10.80.16.3 | root | Xfx2018@)!* |      |

### 测试
|   hostname  |  ip或网址   | 用户 |       密码       | 备注 |
|-------------|-------------|------|------------------|------|
| bssit-cdh-1 | 10.83.80.5  | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-2 | 10.83.80.7  | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-3 | 10.83.80.14 | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-4 | 10.83.80.2  | root | !W$WdwY7U%pe)YkQ |      |
| 数据库mysql | 10.83.96.10 | root | !W$WdwY7U%pe)YkQ |      |







# 2、Linux 操作
## Shell 命令
```shell
# 添加启动视图
# /etc/motd
                   _ooOoo_
                  o8888888o
                  88" . "88
                  (| -_- |)
                  O\  =  /O
               ____/`---'\____
             .'  \\|     |//  `.
            /  \\|||  :  |||//  \
           /  _||||| -:- |||||-  \
           |   | \\\  -  /// |   |
           | \_|  ''\---/''  |   |
           \  .-\__  `-`  ___/-. /
         ___`. .'  /--.--\  `. . __
      ."" '<  `.___\_<|>_/___.'  >'"".
     | | :  `- \`.;`\ _ /`;.`/ - ` : | |
     \  \ `-.   \_ __\ /__ _/   .-` /  /
======`-.____`-.___\_____/___.-`____.-'======
                   `=---='


^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
           佛祖保佑       永不死机
           心外无法       法外无心

# 添加基础操作命令
# /etc/profile
export TIME_STYLE='+%F %T'      # 设置系统默认时间格式为：yyyy-MM-dd HH:mm:ss
alias ll='ls -lh --color=auto'  # 修改 ll 命令带有文件大小
alias la='ll -A'                # 设置 la 命令可以查看到隐藏文件

```


## Kafka 命令
```shell
# 生产者
kafka-console-producer --broker-list bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092 --topic test

# 消费者
kafka-console-consumer --bootstrap-server bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092 --topic test

```


## MySQL 命令
```shell
mysql -h10.80.16.9 -P3306 -uroot -p'!mAkJTMI%lH5ONDw' -Decasdb -s -N -e 'select ORIGINAL_MSG from ecas_msg_log limit 1;'

```


## Hive 命令
```shell
beeline -u jdbc:hive2://node47:10000 -n hive --showHeader=false --outputformat=csv2 -e ''

```





# 3、SQL 语句
## 3.1 通用 SQL
```sql
-- HQL 学习
-- union 与 union all 相比 多了去重排序的功能

-- 表重命名
ALTER TABLE ods_starconnect.07_distiinct_starconnect_actual_repayment_info RENAME TO ods_starconnect.07_distinct_starconnect_actual_repayment_info;

```



## 3.2 Hive SQL 语句
```sql
-- Hive 函数操作
show functions like '*time*';
desc function extended from_unixtime;

drop function encrypt_aes;
drop function decrypt_aes;

hdfs dfs -put ./HiveUDF-1.0.jar /user/hive/auxlib

add jar hdfs:///user/hive/auxlib/qubole-hive-JDBC-0.0.7.jar;

create function encrypt_aes as 'com.weshare.udf.Aes_Encrypt' using jar 'hdfs://node47:8020/user/hive/auxlib/HiveUDF-1.0.jar';
create function decrypt_aes as 'com.weshare.udf.Aes_Decrypt' using jar 'hdfs://node47:8020/user/hive/auxlib/HiveUDF-1.0.jar';

-- 测试范围匹配
-- 严格模式（strict）下禁用笛卡尔积，需要非严格模式（nonstrict）
set hive.mapred.mode = nonstrict;
drop table if exists tmp_test;
create temporary table if not exists tmp_test as
select cast('2020-01-01' as date) as dd union all
select cast('2020-01-02' as date) as dd union all
select cast('2020-01-03' as date) as dd;

select a.dd as aa,b.dd as bb
from tmp_test as a,tmp_test as b
where a.dd <= b.dd and b.dd < date_add(a.dd,2)
order by aa,bb;


-- 测试 Hive 的 Map 数据类型
drop table if exists tmp_test_hivemap;
create temporary table if not exists tmp_test_hivemap as
select cast('{"a":{"aa":"11"}}' as string) as json union all
select cast('{"b":{"bb":"22"}}' as string) as json union all
select cast('{"c":{"cc":"33"}}' as string) as json;

select * from tmp_test_hivemap;
-- select cast(json as map<string,string>) as json from tmp_test_hivemap; -- 不起作用

select
get_json_object(json,'$.a.aa') as aa
from tmp_test_hivemap;


-- 使用 Hive 连接 MySQL 的表
CREATE TEMPORARY EXTERNAL TABLE student_jdbc(
  name string,
  age int,
  gpa double
)
-- STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
-- STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JDBCStorageHandler'
STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://127.0.0.1/test",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "password",
  "hive.sql.table"          = "student_jdbc",
  "hive.sql.dbcp.maxActive" = "1"
);


```



## 3.3 Impala SQL 语句
```sql
-- impala 同步 hive [表] 元数据
invalidate metadata [table];
-- impala 刷新数据库
refresh [table] [partition [partition]];
refresh dwb.dwb_credit_apply;

-- Hive 函数操作
show functions in _impala_builtins like '*date*';

create function encrypt_aes(string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0.jar' symbol='com.weshare.udf.Aes_Encrypt';
create function encrypt_aes(string, string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0.jar' symbol='com.weshare.udf.Aes_Decrypt';

create function decrypt_aes(string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0.jar' symbol='com.weshare.udf.Aes_Encrypt';
create function decrypt_aes(string, string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0.jar' symbol='com.weshare.udf.Aes_Decrypt';

```


# 4、Markdown 操作

![图片](file://D:\soft\desktop\8512a353f8a72ff0565d187592880ef.jpg) <!-- Shift + Win + K -->

[链接](http://www.baidu.com) <!-- Ctrl + Alt + V -->

[引用][引用] <!-- Ctrl + Alt + R 点击快捷键后，直接输入文字即可 -->
[引用]:http://www.baidu.com

注释引用[^1] <!-- Alt + Shift + 6 插入注释 -->
[^1]: http://www.baidu.com

**加粗文本** __加粗文本__

==标记文本==

~~删除文本~~

> 引用文本

H~2~O is是液体。

2^10^ 运算结果是 1024
