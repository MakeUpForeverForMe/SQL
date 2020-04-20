Hive 数据类型
数字类型
  TINYINT          (1-byte signed integer, from -128                 to 127)
  SMALLINT         (2-byte signed integer, from -32768               to 32767)
  INT/INTEGER      (4-byte signed integer, from -2147483648          to 2147483647)
  BIGINT           (8-byte signed integer, from -9223372036854775808 to 9223372036854775807)
  FLOAT            (4-byte single precision floating point number) -- 单精度浮点型数值
  DOUBLE           (8-byte double precision floating point number) -- 双精度浮点型数值
  DOUBLE PRECISION (alias for DOUBLE, only available starting with Hive 2.2.0) -- DOUBLE 的别名 -- available 可用的
  DECIMAL          DECIMAL(precision, scale) -- DECIMAL（精度，刻度）precision 默认 10，scale 默认 0 （即无小数位）
    Introduced in Hive 0.11.0 with a precision of 38 digits   -- 在 Hive 0.11.0 中引入，精度为 38 位
    Hive 0.13.0 introduced user-definable precision and scale -- 在 Hive 0.13.0 中引入了用户可定义的精度和比例
  NUMERIC          (same as DECIMAL, starting with Hive 3.0.0)
日期/时间类型 （使用 timestamp.formats 来支持其他时间戳格式。例如，yyyy-MM-dd'T'HH:mm:ss.SSS，yyyy-MM-dd'T'HH:mm:ss）
  TIMESTAMP (Note: Only available starting with Hive 0.8.0)  -- yyyy-mm-dd hh:mm:ss
  DATE      (Note: Only available starting with Hive 0.12.0) -- yyyy­mm­dd
  INTERVAL  (Note: Only available starting with Hive 1.2.0)  -- 间隔，与NUMERIC不一样。不明白怎么用
String 类型
  STRING  (单引号或双引号引起来的值)
  VARCHAR (Note: Only available starting with Hive 0.12.0) -- 可变长度(1和65535)，如：指定10，不足10为本身，超过截前10
  CHAR    (Note: Only available starting with Hive 0.13.0) -- 固定长度值，最长 255
其他

-- 排序测试
DROP TABLE IF EXISTS base_order_number;
CREATE TEMPORARY TABLE IF NOT EXISTS base_order_number as
select cast(1     as int) as num union all
select cast(11    as int) as num union all
select cast(456   as int) as num union all
select cast(64    as int) as num union all
select cast(765   as int) as num union all
select cast(42    as int) as num union all
select cast(5235  as int) as num union all
select cast(53    as int) as num union all
select cast(523   as int) as num union all
select cast(549   as int) as num;
select num from base_order_number order by num;


DROP TABLE IF EXISTS base;
CREATE TEMPORARY TABLE IF NOT EXISTS base as
select 'zhangsa' as user_id, 'test1'  as device_id, 'new' as user_type, 67.1  as price, 2 as sales union all
select 'lisi'    as user_id, 'test2'  as device_id, 'old' as user_type, 43.32 as price, 1 as sales union all
select 'wanger'  as user_id, 'test3'  as device_id, 'new' as user_type, 88.88 as price, 3 as sales union all
select 'liliu'   as user_id, 'test4'  as device_id, 'new' as user_type, 66.0  as price, 1 as sales union all
select 'tom'     as user_id, 'test5'  as device_id, 'new' as user_type, 54.32 as price, 1 as sales union all
select 'tomas'   as user_id, 'test6'  as device_id, 'old' as user_type, 77.77 as price, 2 as sales union all
select 'tomson'  as user_id, 'test7'  as device_id, 'old' as user_type, 88.44 as price, 3 as sales union all
select 'tom1'    as user_id, 'test8'  as device_id, 'new' as user_type, 56.55 as price, 6 as sales union all
select 'tom2'    as user_id, 'test9'  as device_id, 'new' as user_type, 88.88 as price, 5 as sales union all
select 'tom3'    as user_id, 'test10' as device_id, 'new' as user_type, 66.66 as price, 5 as sales;

SELECT user_id,user_type,price,sales,row_number() over(partition by user_type order by sales,user_id) as row_num,
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales) AS pv1, -- 默认为从起点到当前行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv21,
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv2, --从起点到当前行，结果同pv1
  SUM(price) OVER(PARTITION BY user_type) AS pv3,               --分组内所有行
  SUM(price) OVER(PARTITION BY user_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv31,
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pv4,   --当前行+往前3行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS pv5,    --当前行+往前3行+往后1行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS pv6   ---当前行+往后所有行
from base order by user_type,sales;

-- OVER 从句
OVER 与标准聚合函数 COUNT, SUM, MIN, MAX, AVG
使用 PARTITION BY 语句进行 OVER ，该语句可以对任何原始数据的一个或多个列进行分区。
使用 PARTITION BY 与 ORDER BY 语句进行 OVER ，该语句可以对任何原始数据的一个或多个列进行分区排序。
使用窗口规范，窗口规范支持以下格式： -- num 不可以为 0 ，是正整数
(ROW | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECEDING | CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROW | RANGE) BETWEEN CURRENT ROW                   AND (CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
(ROW | RANGE) BETWEEN [num] PRECEDING               AND (UNBOUNDED | [num]) FOLLOWING
当 ORDER BY 后面缺少窗口从句条件，窗口规范默认是
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
当 ORDER BY 和窗口从句都缺失，窗口规范默认是：
ROWS  BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- 行比较分析函数 lead 和 lag 无 window (窗口)子句。
-- 窗口函数
LEAD(col [,n,DEFAULT]) 用于获取窗口内往下第n行的值。
  列名，往下第 n 行(可选，默认为 1 )，默认值(可选，当往下第 n 行为 NULL 时，取默认值，默认为 NULL )
select
  user_id,
  device_id,
  sales,
  row_number()            over(order by sales) as row_num,
  lead(device_id)         over(order by sales) as after_one_line,
  lead(device_id,2)       over(order by sales) as after_two_line,
  lead(device_id,2,'abc') over(order by sales) as after_two_line_default
from base order by sales;

LAG(col [,n,DEFAULT])  用于获取窗口内往上第n行的值。
  列名，往上第 n 行(可选，默认为 1 )，默认值(可选，当往上第 n 行为 NULL 时，取默认值，默认为 NULL )
select
  user_id,
  device_id,
  sales,
  row_number()           over(order by sales) as row_num,
  lag(device_id)         over(order by sales) as before_one_line,
  lag(device_id,2)       over(order by sales) as before_two_line,
  lag(device_id,2,'abc') over(order by sales) as before_two_line_default
from base order by sales;

FIRST_VALUE 取出分组内排序后，截止到当前行，第一个值。
            window 子句为 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
select
  user_id,
  user_type,
  sales,
  row_number() over(partition by user_type order by sales) as row_num,
  -- 按分区取第一个值
  first_value(user_id) over (partition by user_type order by sales asc)  as min_sales_user,
  first_value(user_id) over (partition by user_type order by sales asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  as min_sales_user_rows,
  -- 按分区取最后一个值
  first_value(user_id) over (partition by user_type order by sales desc) as max_sales_user,
  first_value(user_id) over (partition by user_type order by sales desc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as max_sales_user_rows
from base order by user_type,sales;

LAST_VALUE  取出分组内排序后，截止到当前行，最后一个值。
            window 子句为 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
select
  user_id,
  user_type,
  sales,
  row_number()        over(partition by user_type order by sales,user_id) as row_num,
  last_value(user_id) over(partition by user_type order by sales asc) as last_max_user,
  last_value(user_id) over(partition by user_type order by sales asc  rows  between unbounded preceding and current row)  as last_max_user_row,
  last_value(user_id) over(partition by user_type order by sales asc  range between current row and unbounded following)  as last_max_user_ran,
  last_value(user_id) over(partition by user_type order by sales desc)  as last_min_user,
  last_value(user_id) over(partition by user_type order by sales desc range between current row and unbounded following)  as last_min_user_ran,
  last_value(user_id) over(partition by user_type order by sales desc rows  between current row and unbounded following)  as last_min_user_row
from base order by user_type,sales,row_num;

-- 分析函数
ROW_NUMBER()    从1开始，按照顺序，生成分组内记录的序列

RANK()          生成数据项在分组中的排名，排名相等会在名次中留下空位

DENSE_RANK()    生成数据项在分组中的排名，排名相等会在名次中不会留下空位

NTILE(n)  将分组数据按照顺序切分成n片，返回当前切片值。如果切片不均匀，默认增加第一个切片的分布。
          NTILE不支持ROWS BETWEEN
select
  user_type,sales,
  NTILE(2) OVER(PARTITION BY user_type ORDER BY sales) AS nt2,    -- 分组内将数据分成 2 片
  NTILE(3) OVER(PARTITION BY user_type ORDER BY sales) AS nt3,    -- 分组内将数据分成 3 片
  NTILE(4) OVER(PARTITION BY user_type ORDER BY sales) AS nt4,    -- 分组内将数据分成 4 片
  NTILE(4) OVER(ORDER BY sales)                        AS all_nt4 -- 将所有数据分成 4 片
from base order by user_type,sales;

-- 注意： 序列函数不支持WINDOW子句
CUME_DIST()     小于等于当前值的行数除以分组内总行数 -- 注意的点 小于等于当前值的行数
select
  user_id,user_type,sales,
  --没有partition,所有数据均为1组
  CUME_DIST() OVER(ORDER BY sales) AS cd1,
  --按照user_type进行分组
  CUME_DIST() OVER(PARTITION BY user_type ORDER BY sales) AS cd2
from base order by user_type,sales;

PERCENT_RANK()  分组内当前行的 RANK() 值-1/分组内总行数-1
select
  user_type,sales,
  sum(1)         over(partition by user_type)                as s,    -- 分组内总行数
  rank()         over(order by sales)                        as ar,   -- 全局 rank 值
  percent_rank() over(order by sales)                        as apr,  -- 全局 percent_rank 值
  rank()         over(partition by user_type order by sales) as gr,   -- 分组 rank 值
  percent_rank() over(partition by user_type order by sales) as gprg  -- 分组 percent_rank 值
from base order by user_type,sales;















目前有2张表
  交易明细表，记录每笔交易信息
  产品映射表，记录产品码与具体业务场景的映射关系
其具体结构如下，请根据这2张基础表，写出对应的查询逻辑。
trade_detail 表：（1亿条数据）:
user_id,
trade_no,
product_code,
trade_time

DROP TABLE IF EXISTS trade_detail;
CREATE TEMPORARY TABLE IF NOT EXISTS trade_detail as
select 'zhangsa' as user_id, 'trade_no_1'  as trade_no, 'code_1' as product_code, '2020-01-01 00:00:00' as trade_time union all
select 'lisi'    as user_id, 'trade_no_2'  as trade_no, 'code_5' as product_code, '2020-01-02 00:00:00' as trade_time union all
select 'wanger'  as user_id, 'trade_no_3'  as trade_no, 'code_2' as product_code, '2020-01-03 00:00:00' as trade_time union all
select 'liliu'   as user_id, 'trade_no_4'  as trade_no, 'code_1' as product_code, '2020-01-04 00:00:00' as trade_time union all
select 'tom'     as user_id, 'trade_no_5'  as trade_no, 'code_2' as product_code, '2020-01-05 00:00:00' as trade_time union all
select 'tomas'   as user_id, 'trade_no_6'  as trade_no, 'code_4' as product_code, '2020-01-06 00:00:00' as trade_time union all
select 'tomson'  as user_id, 'trade_no_7'  as trade_no, 'code_2' as product_code, '2020-01-07 00:00:00' as trade_time union all
select 'tom'     as user_id, 'trade_no_8'  as trade_no, 'code_3' as product_code, '2020-01-08 00:00:00' as trade_time union all
select 'tom'     as user_id, 'trade_no_9'  as trade_no, 'code_1' as product_code, '2020-01-09 00:00:00' as trade_time union all
select 'tom3'    as user_id, 'trade_no_10' as trade_no, 'code_3' as product_code, '2020-01-10 00:00:00' as trade_time;

SELECT user_id,trade_no,product_code,trade_time from trade_detail;

dim_product_scene 表：(100条数据):
product_code,
biz_scene

DROP TABLE IF EXISTS dim_product_scene;
CREATE TEMPORARY TABLE IF NOT EXISTS dim_product_scene as
select 'code_1' as product_code, 'biz_scene_1'  as biz_scene union all
select 'code_5' as product_code, 'biz_scene_2'  as biz_scene union all
select 'code_2' as product_code, 'biz_scene_3'  as biz_scene union all
select 'code_1' as product_code, 'biz_scene_1'  as biz_scene union all
select 'code_2' as product_code, 'biz_scene_2'  as biz_scene union all
select 'code_4' as product_code, 'biz_scene_3'  as biz_scene union all
select 'code_2' as product_code, 'biz_scene_1'  as biz_scene union all
select 'code_3' as product_code, 'biz_scene_2'  as biz_scene union all
select 'code_1' as product_code, 'biz_scene_3'  as biz_scene union all
select 'code_3' as product_code, 'biz_scene_1'  as biz_scene;

SELECT product_code,biz_scene from dim_product_scene;


（1）取每个用户在各个biz_scene下的首次支付时间、最后一次支付时间

SELECT biz_scene,user_id,
  first_value(trade_time) over(partition by biz_scene,user_id order by trade_time)      as first_time,
  first_value(trade_time) over(partition by biz_scene,user_id order by trade_time desc) as last_time
from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
order by biz_scene,user_id;

（2）取每个用户在各个biz_scene下的历史第二笔支付时间

SELECT biz_scene,user_id,trade_time
from (
  SELECT biz_scene,user_id,trade_time,row_number() over(partition by biz_scene,user_id order by trade_time) as od
  from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
) as tmp
where od = 2;

（3）计算每个用户的平均支付间隔（即：连续交易的时间间隔，取平均）

SELECT user_id,trade_time,
  LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time) as lead_time,
  datediff(to_date(LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time)),to_date(trade_time)) as date_diff,
  sum(datediff(to_date(LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time)),to_date(trade_time))) over(partition by user_id)/count(1) over(partition by user_id) as avg_time
from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
order by user_id
;

（4）取每个product_code下的总交易用户数（假设超过9000W条数据都是同一个product_code（product_a），需要考虑执行效率。
