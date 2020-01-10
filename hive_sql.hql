mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "CLIENT_INFO" --type=json --fieldFile='/home/hadoop/star_source/ods/export/client_info' -o '/home/hadoop/star_source/out/client_info.json'
mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "EVENT_LOGGER" --type=json --fieldFile='/home/hadoop/star_source/ods/export/event_logger' -o '/home/hadoop/star_source/out/event_logger.json'
mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "PRODUCT_INFO" --type=json --fieldFile='/home/hadoop/star_source/ods/export/product_info' | sed 's/{"$oid"://;s/},/,/' > '/home/hadoop/star_source/out/product_info.json'
mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "RECOMMEND_FLOW" --type=json --fieldFile='/home/hadoop/star_source/ods/export/recommend_flow' -o '/home/hadoop/star_source/out/recommend_flow.json'
hdfs dfs -put -f /home/hadoop/star_source/out/recommend_flow.json /warehouse/weshare/ods_source_new.db/recommend_flow_json
mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "SOURCE_INFO" --type=json --fieldFile='/home/hadoop/star_source/ods/export/source_info' -o '/home/hadoop/star_source/out/source_info.json'
mongoexport -h 10.80.16.34:27017 -u mongouser -p S6gvEdMzYVUT8x -d "starsource" -c "FLOW_RECORD" --type=json --fieldFile='/home/hdfs/star_source/ods/export/flow_record' -o '/home/hdfs/star_source/out/flow_record.json'
hdfs dfs -put -f /home/hdfs/star_source/out/flow_record.json /warehouse/tablespace/managed/hive/ods_source_new.db/flow_record_json



beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/behavior_analysis.hql 1> /dev/null
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/per_hour_source_pull.hql 1> /dev/null
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/recommend_product_push.hql 1> /dev/null
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/recommend_source_product_push.hql 1> /dev/null
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/source_city_exp_pull.hql 1> /dev/null
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs -f ~/star_source/dm/sum_pull_push.hql 1> /dev/null






beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.sum_pull_push' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/sum_pull_push.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate sum_pull_push;load data local infile '/home/hdfs/star_source/mysql/sum_pull_push.csv' into table sum_pull_push fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.source_city_exp_pull' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/source_city_exp_pull.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate source_city_exp_pull;load data local infile '/home/hdfs/star_source/mysql/source_city_exp_pull.csv' into table source_city_exp_pull fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.per_hour_source_pull' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/per_hour_source_pull.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate per_hour_source_pull;load data local infile '/home/hdfs/star_source/mysql/per_hour_source_pull.csv' into table per_hour_source_pull fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.recommend_product_push' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/recommend_product_push.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate recommend_product_push;load data local infile '/home/hdfs/star_source/mysql/recommend_product_push.csv' into table recommend_product_push fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.recommend_source_product_push' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/recommend_source_product_push.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate recommend_source_product_push;load data local infile '/home/hdfs/star_source/mysql/recommend_source_product_push.csv' into table recommend_source_product_push fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.behavior_analysis' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/behavior_analysis.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate behavior_analysis;load data local infile '/home/hdfs/star_source/mysql/behavior_analysis.csv' into table behavior_analysis fields terminated by ',';"

beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs --hivevar startDate=20190101000000 --hivevar endDate=20191231000000 --showHeader=false --outputformat=csv2 -e 'select * from dm_cf.access_proportion' | sed '/^\s*$/d' > /home/hdfs/star_source/mysql/access_proportion.csv
mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate access_proportion;load data local infile '/home/hdfs/star_source/mysql/access_proportion.csv' into table access_proportion fields terminated by ',';"






-- 输出不同价格的和及占比
select `name`,sum(if(price>=2,price,0))as p1,sum(if(price<2,price,0))as p2,sum(if(price>=2,price,0))/sum(if(price<2,price,0)) p3 from a;
-- 好分期1万以下，1万到3万，3万到5万，5万以上，分别多少量，占比多少
select
a.datetime,a.org_name,a.pull_less100,a.pull_less100_proportion,a.pull_100_500,a.pull_100_500_proportion,a.pull_more500,a.pull_more500_proportion,a.pull_count,
b.push_less100,b.push_less100_proportion,b.push_100_500,b.push_100_500_proportion,b.push_more500,b.push_more500_proportion,b.push_count
from
(select replace(to_date(a.apply_date),'-','') datetime,c.org_name,
  sum(if(a.expectation<=100,1,0)) pull_less100,sum(if(a.expectation<=100,1,0))/count(distinct a.client_id) pull_less100_proportion,
  sum(if(a.expectation>100 and a.expectation<=500,1,0)) pull_100_500,sum(if(a.expectation>100 and a.expectation<=500,1,0))/count(distinct a.client_id) pull_100_500_proportion,
  sum(if(a.expectation>500,1,0)) pull_more500,sum(if(a.expectation>500,1,0))/count(distinct a.client_id) pull_more500_proportion,
  count(distinct a.client_id) pull_count
  from (select * from ods_source.client_info where company_code = 10013 and spark_job_number = 1380 and replace(to_date(apply_date),'-','') between '20190320' and '20190327') a
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = 1380)
  group by replace(to_date(a.apply_date),'-',''),c.org_name) a
join
(select b.date datetime,c.org_name,
  sum(if(a.expectation<=100,1,0)) push_less100,sum(if(a.expectation<=100,1,0))/count(distinct a.client_id) push_less100_proportion,
  sum(if(a.expectation>100 and a.expectation<=500,1,0)) push_100_500,sum(if(a.expectation>100 and a.expectation<=500,1,0))/count(distinct a.client_id) push_100_500_proportion,
  sum(if(a.expectation>500,1,0)) push_more500,sum(if(a.expectation>500,1,0))/count(distinct a.client_id) push_more500_proportion,
  count(distinct a.client_id) push_count
  from (select * from ods_source.client_info where company_code = 10013 and spark_job_number = 1380) a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = 1380 and b.date between '20190320' and '20190327')
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = 1380)
  group by b.date,c.org_name) b on a.datetime = b.datetime
order by datetime;



select replace(to_date(a.apply_date),'-','') datetime,a.city,b.org_name,
sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),a.city,b.org_name) pull_sum_org,
sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),a.city) pull_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190326' and '20190331') a
join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
group by datetime,city,org_name
order by datetime,city,org_name;



select replace(to_date(apply_date),'-','') datetime,
count(client_id) pull_sum_org,
count(distinct client_id) pull_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') = '20190331' and company_code = 10001)
group by datetime;


-- 有效接收数与有效推送(其他)
select
a.org_name,product_name,a.city,pull_sum_city,pull_sum,push_sum_city,push_sum_product,push_sum,${spark_job_number},a.datetime
from
(select replace(to_date(a.apply_date),'-','') datetime,b.org_name,a.city,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name,a.city) pull_sum_city,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code !=10009 and company_code !=10001 and replace(to_date(apply_date),'-','') between '20190331' and '20190411') a
  join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,city) a
join
(select b.date datetime,c.org_name,d.product_name,a.city,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name,a.city) push_sum_city,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name) push_sum_product,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code !=10009 and company_code !=10001) a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.date between '20190331' and '20190411')
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,product_name,city) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.city = b.city)



select
a.datetime,
a.org_name,
a.route,
pull_sum_route,
push_sum_route,
pull_sum,
push_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  b.org_name,
  if(a.route = 'A' or a.route = 'B',a.route,a.route) route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name,a.route) pull_sum_route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code =10001 and replace(to_date(apply_date),'-','') = '20190423') a
  join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,route) a
join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  c.org_name,
  if(a.route = 'A' or a.route = 'B',a.route,a.route) route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),c.org_name,a.route) push_sum_route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),c.org_name) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code =10001 and replace(to_date(apply_date),'-','') = '20190423') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,route) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.route = b.route)
order by datetime,org_name;


-- 有效接收数与有效推送数(因特力1)
select
a.org_name,product_name,a.city,a.route,pull_sum_route,pull_sum_city,pull_sum,push_sum_route,push_sum_city,push_sum_product,push_sum,${spark_job_number},a.datetime
from
(select replace(to_date(a.apply_date),'-','') datetime,b.org_name,a.city,if(a.route = 'A' or a.route = 'B',a.route,null) route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name,a.city,a.route) pull_sum_route,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name,a.city) pull_sum_city,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code =10001 and replace(to_date(apply_date),'-','') between '20190331' and '20190411') a
  join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,city,route) a
join
(select b.date datetime,c.org_name,d.product_name,a.city,if(a.route = 'A' or a.route = 'B',a.route,null) route,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name,a.city,a.route) push_sum_route,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name,a.city) push_sum_city,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name) push_sum_product,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code =10001) a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.date between '20190331' and '20190411')
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,product_name,city,route) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.city = b.city and a.route = b.route);




-- 有效接收数与有效推送数(因特力代投放)
select
a.org_name,b.product_name,a.city,a.source,a.pull_source_sum,a.pull_city_sum,a.pull_sum,b.push_source_sum,b.push_city_sum,b.push_product_sum,b.push_sum,${spark_job_number},a.datetime
from
(select replace(to_date(a.apply_date),'-','') datetime,b.org_name,a.city,a.source,
  count(distinct a.client_id) pull_source_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),a.city) pull_city_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10009 and replace(to_date(apply_date),'-','') between '20190331' and '20190411') a
  join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by replace(to_date(a.apply_date),'-',''),b.org_name,a.city,a.source) a
join
(select b.date datetime,c.org_name,d.product_name,a.city,a.source,
  count(distinct a.client_id) push_source_sum,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name,a.city) push_city_sum,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name,d.product_name) push_product_sum,
  sum(count(distinct a.client_id)) over(partition by b.date,c.org_name) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10009) a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.date between '20190331' and '20190411')
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by b.date,c.org_name,d.product_name,a.city,a.source) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.city = b.city and a.source = b.source)
order by datetime,org_name,product_name,city,source;




-- 创建MySQL数据库
CREATE TABLE IF NOT EXISTS dm_yinteli(
  datetime VARCHAR(100) comment '日期',
  org_name VARCHAR(100) comment '流量方名称',
  product_name VARCHAR(100) comment '产品方名称',
  city VARCHAR(100) comment '用户所在城市',
  route VARCHAR(100) comment '独家或非独家',
  pull_sum_route int comment '同一天同一流量方同一城市独家或非独家的接收数',
  pull_sum_city int comment '同一天同一流量方同一城市的接收数',
  pull_sum int comment '同一天同一流量方的接收数',
  push_sum_route int comment ''同一天同一流量方同一产品方同一城市独家或非独家的',推送数',
  push_sum_city int comment '同一天同一流量方同一产品方同一城市的推送数',
  push_sum_product int comment '同一天同一流量方同一产品方的推送数',
  push_sum int comment '同一天同一流量方的推送数',
  spark_job_number int)
DEFAULT CHARSET=utf8;


-- 写入表（活动分区）
INSERT overwrite TABLE dm_cf.'dm_opera_distribution_da',ily PARTITION (date)
distribute by apply_date;


INSERT INTO TABLE dm_cf.dm_total_kpi
select
'day',
a.datetime,
planned_revenue,
0 actual_revenue,
planned_cost,
0 actual_cost,
(planned_revenue-planned_cost) planned_profit,
0 actual_profit,
(select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) total_users,
(select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') = '20190331') new_users,
(planned_revenue-planned_cost)/(select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) planned_ARPU,
0 actual_ARPU,
planned_cost/(select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) planned_ACPU,
0 actual_ACPU,
(select count(distinct org_code) from ods_source.org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) total_org,
(select count(distinct org_code) from ods_source.org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and from_unixtime(create_time/1000,'yyyyMMdd') = '20190331') new_org,
(planned_revenue-planned_cost)/(select count(distinct org_code) from ods_source.org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) planned_ARPA,
0 actual_ARPA,
planned_cost/(select count(distinct org_code) from ods_source.org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) planned_ACPA,
0 actual_ACPA
from
(select distinct datetime,sum(amount) planned_revenue
  from
  (select replace(to_date(a.apply_date),'-','') datetime,cast(count(distinct a.client_id) as DOUBLE)*c.price amount
    from (select * from ods_source.client_info where replace(to_date(apply_date),'-','') = '20190331' and spark_job_number = (select max(spark_job_number) from ods_source.client_info)) a
    join ods_source.recommend_recon b
    on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.recommend_ret = 200)
    join ods_source.product_info c
    on (b.product_code = c.product_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    group by datetime,c.price)
  group by datetime) a
join
(select replace(to_date(apply_date),'-','') datetime,sum(distinct case
  when company_code = 10009
  then (select count(distinct order_id)*25
    from ods_source.event_logger
    where event = 1 and date = '20190331' and company_code = 10009 and spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  when company_code not in (10001,10009)
  then (select sum(*) from (select cast(count(distinct a.order_id) as DOUBLE)*b.price
    from (select * from ods_source.event_logger where event = 2 and date = '20190331' and spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code not in (10001,10009)) a
    join ods_source.source_org_info b
    on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    group by date,org_name,price))
  when company_code = 10001
  then (select count(distinct a.client_id)*(select price from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_code = 10001)
    from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') = '20190331' and company_code = 10001) a
    join ods_source.recommend_recon b
    on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info)))
  end) planned_cost
from ods_source.client_info
where replace(to_date(apply_date),'-','') = '20190331' and spark_job_number = (select max(spark_job_number) from ods_source.client_info)
group by datetime) b
on a.datetime = b.datetime;

INSERT INTO TABLE dm_cf.dm_total_kpi
select
'week',
'20190401',
sum(planned_revenue),
sum(actual_revenue),
sum(planned_cost),
sum(actual_cost),
sum(planned_profit),
sum(actual_profit),
max(total_users),
sum(new_users),
sum(planned_arpu),
sum(actual_arpu),
sum(planned_acpu),
sum(actual_acpu),
max(total_org),
sum(new_org),
sum(planned_arpa),
sum(actual_arpa),
sum(planned_acpa),
sum(actual_acpa)
from (select * from dm_cf.dm_total_kpi where spark_job_number = '(select max(spark_job_number) from ods_source.client_info)' and datetime between from_unixtime(unix_timestamp('20190401','yyyyMMdd')-unix_timestamp('07','dd'),'yyyyMMdd') and '20190401' and pmod(datediff(from_unixtime(unix_timestamp('20190401','yyyyMMdd'),'yyyy-MM-dd'), '2019-03-31'), 7) = '0');

INSERT INTO TABLE dm_cf.dm_total_kpi
select
'month',
'20190401',
sum(planned_revenue),
sum(actual_revenue),
sum(planned_cost),
sum(actual_cost),
sum(planned_profit),
sum(actual_profit),
max(total_users),
sum(new_users),
sum(planned_arpu),
sum(actual_arpu),
sum(planned_acpu),
sum(actual_acpu),
max(total_org),
sum(new_org),
sum(planned_arpa),
sum(actual_arpa),
sum(planned_acpa),
sum(actual_acpa)
from (select * from dm_cf.dm_total_kpi where spark_job_number =(select max(spark_job_number) from ods_source.client_info) and datetime between replace(date_sub(date_sub(from_unixtime(unix_timestamp('20190331','yyyyMMdd'),'yyyy-MM-dd'),1),dayofmonth(date_sub(from_unixtime(unix_timestamp('20190331','yyyyMMdd'),'yyyy-MM-dd'),1))-1),'-','') and '20190331' and replace(last_day(from_unixtime(unix_timestamp('20190331','yyyyMMdd'),'yyyy-MM-dd')),'-','') = '20190331');


DROP TABLE dm_cf.dm_total_kpi;
DROP TABLE dm_cf.'dm_yinteli_flow_subchann',el;
DROP TABLE dm_cf.dm_conversion_data_flow;

select * from dm_cf.dm_total_kpi;
select * from dm_cf.'dm_yinteli_flow_subchann',el;
select * from dm_cf.dm_conversion_data_flow;


DROP TABLE dm_cf.dm_product_settlement;

select * from dm_cf.dm_product_settlement;

INSERT overwrite TABLE dm_cf.dm_total_kpi select * from dm_cf.dm_total_kpi where total_users not like 'null';


INSERT INTO TABLE dm_cf.dm_conversion_data_flow
select
'day',a.date,a.eventcount event2count,null count,b.eventcount event3count,c.retcount ret200count,1011
from
(select date,count(distinct task_id) eventcount from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190402' and event = 2 group by date) a
join
(select date,count(distinct task_id) eventcount from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190402' and event = 3 group by date) b on a.date = b.date
join
(select date,count(distinct client_id) retcount from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190402' and recommend_ret = 200 group by date) c on a.date = c.date;


select distinct * from dm_cf.dm_conversion_data_flow;

select
a.datetime,handsQ,others
from
(select replace(to_date(apply_date),'-','') datetime,count(distinct client_id) handsQ from ods_source.client_info where source in ('500','YDD-25','YDD-26') and company_code = '10001' and replace(to_date(apply_date),'-','') = '20190403' group by datetime) a
join
(select replace(to_date(apply_date),'-','') datetime,count(distinct client_id) others from ods_source.client_info where source not in ('500','YDD-25','YDD-26') and company_code = '10001' and replace(to_date(apply_date),'-','') = '20190403' group by datetime) b
on (a.datetime = b.datetime);



select
a.datetime,handsQ,others
from
(select replace(to_date(a.apply_date),'-','') datetime,count(distinct a.client_id)*(select distinct price from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_code = 10001) handsQ
  from
  (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = '10001' and replace(to_date(apply_date),'-','') = '20190403' and source in ('500','YDD-25','YDD-26')) a
  join
  ods_source.recommend_recon b
  on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.recommend_ret = 200)
  group by datetime) a
join
(select replace(to_date(a.apply_date),'-','') datetime,count(distinct a.client_id)*(select distinct price from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_code = 10001) others
  from
  (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = '10001' and replace(to_date(apply_date),'-','') = '20190403' and source not in ('500','YDD-25','YDD-26')) a
  join
  ods_source.recommend_recon b
  on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.recommend_ret = 200)
  group by datetime) b
on a.datetime = b.datetime;



select
date,c.product_name,each_value,push_person,pull_dis_person
from
(select date,product_code,(select planned_revenue from dm_cf.dm_total_kpi where datetime = '20190403')/count(distinct client_id) each_value,count(if(recommend_ret = 200,client_id,null)) push_person from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190403' group by date,product_code) a
join
(select product_code,count(distinct client_id) pull_dis_person from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190403' and recommend_ret = 200 group by product_code) b
on a.product_code = b.product_code
join
ods_source.product_info c on (a.product_code = c.product_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
order by product_name;


select * from dm_cf.dm_product_settlement;
select * from dm_cf.dm_conversion_data_flow;


select d.product_name,count(distinct a.client_id)
from (select * from ods_source.client_info where spark_job_number = 6342 and replace(to_date(apply_date),'-','') between '20190403' and '20190404') a
join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = 6342)
join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = 6342 and d.product_name = '易贷网')
group by d.product_name;

select
replace(to_date(a.apply_date),'-','') datetime,
c.org_name,
e.org_name,
a.source,
count(distinct a.client_id),
sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),e.org_name) pull_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
join ods_source.recommend_recon b on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
group by datetime,c.org_name,e.org_name,a.source
order by datetime,c.org_name,e.org_name,a.source;


select
replace(to_date(a.apply_date),'-','') datetime,
c.org_name,
e.org_name,
a.source,
count(distinct a.client_id),
sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),e.org_name) push_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
group by datetime,c.org_name,e.org_name,a.source
order by datetime,c.org_name,e.org_name,a.source;


select
replace(to_date(a.apply_date),'-','') datetime,c.org_name,
sum(if(a.expectation < 100,1,0)) price_le100,sum(if(a.expectation < 100,1,0))/count(distinct a.client_id) price_le100_p,
sum(if(a.expectation between 100 and 500,1,0)) price_gt100_le500,sum(if(a.expectation between 100 and 500,1,0))/count(distinct a.client_id) price_gt100_le500_p,
sum(if(a.expectation > 500,1,0)) price_gt500,sum(if(a.expectation > 500,1,0))/count(distinct a.client_id) price_gt500_p,
count(distinct a.client_id) sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
join ods_source.recommend_recon b on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
group by datetime,c.org_name
order by datetime,c.org_name;




select
replace(to_date(a.apply_date),'-','') datetime,c.org_name,
sum(if(a.expectation < 100,1,0)) price_le100,sum(if(a.expectation < 100,1,0))/count(distinct a.client_id) price_le100_p,
sum(if(a.expectation between 100 and 500,1,0)) price_gt100_le500,sum(if(a.expectation between 100 and 500,1,0))/count(distinct a.client_id) price_gt100_le500_p,
sum(if(a.expectation > 500,1,0)) price_gt500,sum(if(a.expectation > 500,1,0))/count(distinct a.client_id) price_gt500_p,
count(distinct a.client_id) sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
group by datetime,c.org_name
order by datetime,c.org_name;


select
a.datetime,
a.product_name,
a.org_name,
a.city,
pull_city_sum,
push_city_sum,
pull_org_sum,
push_org_sum,
pull_product_sum,
push_product_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  d.product_name,
  c.org_name,
  a.city,
  count(distinct a.client_id) pull_city_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),d.product_name,c.org_name) pull_org_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),d.product_name) pull_product_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190409') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and product_name = '速易')
  group by datetime,d.product_name,c.org_name,a.city) a
left join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  d.product_name,
  c.org_name,
  a.city,
  count(distinct a.client_id) push_city_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),d.product_name,c.org_name) push_org_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),d.product_name) push_product_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190409') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and product_name = '速易')
  group by datetime,d.product_name,c.org_name,a.city) b
on (a.datetime = b.datetime and a.product_name = b.product_name and a.org_name = b.org_name and a.city = b.city)
order by datetime,product_name,org_name,city;


select
a.datetime,
a.city,
a.org_name,
pull_org_sum,
push_org_sum,
pull_city_sum,
push_city_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  a.city,
  c.org_name,
  count(distinct a.client_id) pull_org_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) pull_city_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') = '20190409' and city = '上海') a
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,a.city,c.org_name) a
left join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  a.city,
  c.org_name,
  count(distinct a.client_id) push_org_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) push_city_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') = '20190409' and city = '上海') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,a.city,c.org_name) b
on (a.datetime = b.datetime and a.city = b.city and a.org_name = b.org_name)
order by datetime,city,org_name;

select distinct product_name from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and product_name = '速易';


select
a.datetime,
a.org_name,
a.source_org_name,
pull_sum_source_org,
push_sum_source_org,
jump_rate,
pull_sum_product_org,
push_sum_product_org
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  e.org_name,
  c.org_name source_org_name,
  count(distinct a.client_id) pull_sum_source_org,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),e.org_name) pull_sum_product_org
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
  group by datetime,e.org_name,source_org_name) a
left join
(select
  a.datetime,
  a.org_name,
  a.source_org_name,
  push_sum_source_org,
  push_sum_product_org,
  jump_rate
  from
  (select
    replace(to_date(a.apply_date),'-','') datetime,
    e.org_name,
    c.org_name source_org_name,
    count(distinct a.client_id) push_sum_source_org,
    sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),e.org_name) push_sum_product_org
    from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190315' and '20190423') a
    join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    join ods_source.org_info e on (d.org_code = e.org_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and e.org_name = '掌众')
    group by datetime,e.org_name,source_org_name) a
  join
  (select
    a.date datetime,
    f.org_name,
    d.org_name source_org_name,
    count(distinct a.client_id) jump_rate
    from (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date between '20190315' and '20190423') a
    join ods_source.event_logger b on (a.task_id = b.task_id and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and b.event = 100)
    join ods_source.client_info c on (a.client_id = c.client_id and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    left join ods_source.source_org_info d on (c.company_code = d.org_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    join ods_source.product_info e on (a.product_code = e.product_code and e.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
    join ods_source.org_info f on (e.org_code = f.org_code and f.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and f.org_name = '掌众')
    group by a.date,f.org_name,source_org_name) b
  on (a.datetime = b.datetime and a.org_name = b.org_name and a.source_org_name = b.source_org_name)) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.source_org_name = b.source_org_name)
order by datetime,org_name,source_org_name;



-- 点点与点点金融
select
a.datetime,
a.org_name,
a.product_name,
a.source,
pull_source_sum,
push_source_sum,
pull_sum_org_product,
push_sum_org_product,
pull_sum_org,
push_sum_org,
pull_sum,
push_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  b.org_name,
  d.product_name,
  a.source,
  count(distinct a.client_id) pull_source_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name,d.product_name) pull_sum_org_product,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),b.org_name) pull_sum_org,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190423' and '20190414') a
  join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.recommend_recon c on (a.client_id = c.client_id and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (c.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and d.product_name = '点点' or d.product_name = '点点金融')
  group by datetime,b.org_name,d.product_name,a.source) a
join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  c.org_name,
  d.product_name,
  a.source,
  count(distinct a.client_id) push_source_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),c.org_name,d.product_name) push_sum_org_product,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-',''),c.org_name) push_sum_org,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190423' and '20190414') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.product_info d on (b.product_code = d.product_code and d.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and d.product_name = '点点' or d.product_name = '点点金融')
  group by datetime,c.org_name,d.product_name,a.source) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.product_name = b.product_name and (a.source = b.source or a.source is null))
order by datetime,org_name,product_name,source;



-- 额度在5k、10k间的比例有产品级
select
a.datetime,
a.org_name,
pull_price_le5k,
push_price_le5k,
pull_price_le5k_p,
push_price_le5k_p,
push_price_gt5k_city,
pull_price_gt5k_le10k,
pull_price_gt5k_le10k_p,
push_price_gt5k_le10k_p,
pull_price_gt10k,
push_price_gt10k,
pull_price_gt10k_p,
push_price_gt10k_p,
pull_sum,
push_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  c.org_name,
  sum(if(a.expectation <= 0.5,1,0)) pull_price_le5k,
  sum(if(a.expectation <= 0.5,1,0))/count(distinct a.client_id) pull_price_le5k_p,
  sum(if(a.expectation > 0.5 and a.expectation <= 1,1,0)) pull_price_gt5k_le10k,
  sum(if(a.expectation > 0.5 and a.expectation <= 1,1,0))/count(distinct a.client_id) pull_price_gt5k_le10k_p,
  sum(if(a.expectation > 1,1,0)) pull_price_gt10k,
  sum(if(a.expectation > 1,1,0))/count(distinct a.client_id) pull_price_gt10k_p,
  count(distinct a.client_id) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190101' and '20190418') a
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_name in ('因特力1','因特力代投放','好分期'))
  group by datetime,org_name) a
left join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  c.org_name,
  sum(if(a.expectation <= 0.5,1,0)) push_price_le5k,
  sum(if(a.expectation <= 0.5,1,0))/count(distinct a.client_id) push_price_le5k_p,
  sum(if(a.expectation > 0.5 and a.expectation <= 0,1,0)) push_price_gt5k_city,
  sum(if(a.expectation > 0.5 and a.expectation <= 0,1,0))/count(distinct a.client_id) push_price_gt5k_le10k_p,
  sum(if(a.expectation > 1,1,0)) push_price_gt10k,
  sum(if(a.expectation > 1,1,0))/count(distinct a.client_id) push_price_gt10k_p,
  count(distinct a.client_id) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190101' and '20190418') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_name in ('因特力1','因特力代投放','好分期'))
  group by datetime,org_name) b
on (a.datetime = b.datetime and a.org_name = b.org_name)
order by datetime,org_name;

-- 最近一周上海、北京、深圳、南京这4个城市目前接收流量方给我们的量级情况
select
replace(to_date(apply_date),'-','') datetime,
client_info.city,
count(distinct client_info.client_id)
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190423' and '20190414' and (city = '上海' or city = '北京' or city = '深圳' or city = '南京')) client_info
group by datetime,city
order by datetime,city;

-- 对了，麻烦帮我拉下北京、上海、广州、西安、杭州、苏州、义乌这7个城市，4月份到目前的各城市的有效推送数据，多谢
select
replace(to_date(apply_date),'-','') datetime,
client_info.city city,
count(distinct client_info.client_id) push_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190423' and (city = '上海' or city = '北京' or city = '广州' or city = '西安' or city = '杭州' or city = '苏州' or city = '义乌')) client_info
join ods_source.recommend_recon recommend_recon
on (client_info.client_id = recommend_recon.client_id and recommend_recon.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_recon.recommend_ret = 200)
group by datetime,city
order by datetime,city;

select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190423' and city like '%义乌%' limit 1;


select * from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) client_info
join ods_source.source_org_info
on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and source_org_info.org_code = 10009) limit 10;


-- 因特力的有效推送
select
sum(push_sum)
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  a.company_code,
  a.source,
  count(distinct a.client_id) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code in (10001,10009) and replace(to_date(apply_date),'-','') between '20190401' and '20190424') a
  join ods_source.recommend_recon c on (a.client_id = c.client_id and c.recommend_ret = 200 and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,company_code,source) a;


-- 所有渠道的有效推送
select
sum(push_sum)
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  a.source,
  count(distinct a.client_id) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190424') a
  join ods_source.recommend_recon c on (a.client_id = c.client_id and c.recommend_ret = 200 and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,source) a;



-- 因特力1的有效推送
select
replace(to_date(a.apply_date),'-','') datetime,
b.org_name,
a.route,
count(distinct a.client_id) pull_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10001 and replace(to_date(apply_date),'-','') between '20190423' and '20190424') a
join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.recommend_recon c on (a.client_id = c.client_id and c.recommend_ret = 200 and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
group by datetime,org_name,route
order by datetime,org_name,route;


-- 因特利代投放8-10号的 按照通路来的 有效接收和有效推送数
select
a.datetime,
a.org_name,
a.source,
a.pull_source_sum,
a.pull_sum,
b.push_source_sum,
b.push_sum
from
(select
  replace(to_date(a.apply_date),'-','') datetime,
  b.org_name,
  a.source,
  count(distinct a.client_id) pull_source_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10009 and replace(to_date(apply_date),'-','') between '20190423' and '20190423') a
  left join ods_source.source_org_info b on (a.company_code = b.org_code and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,b.org_name,a.source) a
left join
(select
  replace(to_date(a.apply_date),'-','') datetime,
  c.org_name,
  a.source,
  count(distinct a.client_id) push_source_sum,
  sum(count(distinct a.client_id)) over(partition by replace(to_date(a.apply_date),'-','')) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10009 and replace(to_date(apply_date),'-','') between '20190423' and '20190423') a
  join ods_source.recommend_recon b on (a.client_id = b.client_id and b.recommend_ret = 200 and b.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  left join ods_source.source_org_info c on (a.company_code = c.org_code and c.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,c.org_name,a.source) b
on (a.datetime = b.datetime and a.org_name = b.org_name and a.source = b.source)
order by datetime,org_name,source;


# 帮忙拉一下4.1-4.23   杭州，北京 南京 上海  苏州  5万以上 每天有多少接收量
select
replace(to_date(a.apply_date),'-','') datetime,
a.city,
sum(if(a.expectation > 5,1,0)) pull_price_gt50k,
sum(if(a.expectation > 5,1,0))/count(distinct a.client_id) pull_price_gt50k_p,
count(distinct a.client_id) pull_sum
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190423' and city in ('杭州','北京','南京','上海','苏州')) a
group by datetime,city
order by datetime,city;


-- 金额：5万以上 地域：北京或上海 资产信息：公积金已缴纳或公积金满三年  或  有房无贷款  或  有房有贷  或  有车无贷  或 有车有贷
select * from CLIENT_INFO where date_format(apply_date,'%Y%m%d%') = '2019-04-19' and city in ("北京","上海") and (accumulationfund in (1,3) or have_house in (1,2) or have_car in (1,2)) and EXPECTATION > 5;
select * from ods_source.client_info where to_date(apply_date) = '2019-04-19' and city in ("北京","上海") and (accumulationfund in (1,3) or have_house in (1,2) or have_car in (1,2)) and expectation > 5;

select count(distinct client_id) from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date between '20190101' and '20190423';

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) between '2019-01-01' and '2019-04-21';


select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) = '2019-04-23' and company_code is null;

select distinct org_code,org_name,from_unixtime(create_time/1000,'yyyy-MM-dd') from ods_source.source_org_info order by org_code;

select distinct from_unixtime(time/1000,'yyyy-MM-dd HH:mm:ss'),date from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) limit 2;

select * from int_party.source_map limit 2;

select from_unixtime(create_time/1000,'yyyy-MM-dd HH:mm:ss'),from_unixtime(update_time/1000,'yyyy-MM-dd HH:mm:ss') from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) = '2019-04-23' and company_code is null;


select from_unixtime(create_time/1000,'yyyy-MM-dd HH:mm:ss') from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) limit 10;

select min(to_date(apply_date)) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);



-- 4.1-4.24每天流量方推送给每个产品的 有效推送  有效接收
select
pull.datetime,
pull.org_name,
product_name,
pull_sum,
push_product_sum,
push_sum
from
(select
  to_date(apply_date) datetime,
  org_name,
  count(distinct client_id) pull_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190424') client_info
  left join ods_source.source_org_info source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name) pull
left join
(select
  to_date(apply_date) datetime,
  org_name,
  product_name,
  count(distinct client_info.client_id) push_product_sum,
  sum(count(distinct client_info.client_id)) over(partition by to_date(apply_date),org_name) push_sum
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190424') client_info
  left join ods_source.source_org_info source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  join ods_source.recommend_recon recommend_recon on (client_info.client_id = recommend_recon.client_id and recommend_recon.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200)
  left join ods_source.product_info product_info on (recommend_recon.product_code = product_info.product_code and product_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
  group by datetime,org_name,product_name) push
on (pull.datetime = push.datetime and pull.org_name = push.org_name)
order by datetime,org_name,product_name;

-- 查下4.1-4.24 每个产品的接收量对应的流量方的额度，城市,人群年龄(产品方为主，有哪些流量给她，城市 额度 年龄段)
select
to_date(apply_date) datetime,
product_name,
org_name,
city,
expectation,
age
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(apply_date),'-','') between '20190401' and '20190424') client_info
left join ods_source.source_org_info source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.recommend_recon recommend_recon on (client_info.client_id = recommend_recon.client_id and recommend_recon.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200)
left join ods_source.product_info product_info on (recommend_recon.product_code = product_info.product_code and product_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
group by datetime,product_name,org_name,city,expectation,age
order by datetime,product_name,org_name,city,expectation,age;

-- 因特力1有效接收数
select
org_name,
to_date(apply_date) datetime,
count(distinct client_info.client_id)
from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10001 and to_date(apply_date) = '2019-04-24') client_info
left join ods_source.source_org_info source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info))
join ods_source.recommend_recon recommend_recon on (client_info.client_id = recommend_recon.client_id and recommend_recon.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_recon.recommend_ret = 200)
group by org_name,datetime;

-- 接收数
select shll.date,shll.company_code,org_name,sum,sum_pull,sum_push from
(select date,company_code,count(distinct order_id) sum from (select * from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190427') event_logger group by date,company_code) shll
left join
(select date,company_code,count(distinct order_id) sum_pull from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and event >= 3 and date = '20190427' group by date,company_code) pull
on (shll.date = pull.date and shll.company_code = pull.company_code)
left join
(select event_logger.date,company_code,count(distinct recommend_recon.client_id) sum_push from (select * from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190427') event_logger join ods_source.recommend_recon recommend_recon on event_logger.task_id = recommend_recon.task_id where recommend_recon.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_recon.recommend_ret = 200 group by event_logger.date,company_code) push
on (shll.date = push.date and shll.company_code = push.company_code)
left join ods_source.source_org_info source_org_info on shll.company_code = source_org_info.org_code where source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info)
order by date,company_code,org_name;

-- 黑牛保险的有效接收与推送
select count(client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10011;

-- 上周（4月22-28）我们总共新注册的人数是多少个
select to_date(apply_date) datetime,count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) between '2019-04-22' and '2019-04-28' group by datetime order by datetime;

-- 星云与星连单表统计
select count(distinct phone_num) from ods_link.t_associates_info where spark_job_number = (select max(spark_job_number) from ods_link.t_associates_info);
select count(distinct id) from ods_link.t_associates_info where spark_job_number = (select max(spark_job_number) from ods_link.t_associates_info);
select count(distinct phone_num) from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info);
select count(distinct custcode) from ods_bank.hisassetbasicinfonew where spark_job_number = (select max(spark_job_number) from ods_bank.hisassetbasicinfonew);
select count(distinct certificatenumber) from ods_bank.hisassetbasicinfonew where spark_job_number = (select max(spark_job_number) from ods_bank.hisassetbasicinfonew);


select total_overdue_daynum,count(distinct serial_number) from ods_link.t_loan_contract_info where spark_job_number = (select max(spark_job_number) from ods_link.t_loan_contract_info) group by total_overdue_daynum order by total_overdue_daynum;



select count(distinct tel) from ods_bank.customerinfo where spark_job_number = (select max(spark_job_number) from ods_bank.customerinfo);
select ishappenoverdue,count(distinct custcode) from ods_bank.hisassetbasicinfonew where spark_job_number = (select max(spark_job_number) from ods_bank.hisassetbasicinfonew) group by ishappenoverdue order by ishappenoverdue;


select count(distinct phone_num) from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info) and PHONE_NUM is not null and BORROWER_NAME is not null and DOCUMENT_NUM is not null;


-- 星连是否逾期
select
total_overdue_daynum,count(distinct phone_num)
from (select * from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info)) a
join (select * from ods_link.t_loan_contract_info where spark_job_number = (select max(spark_job_number) from ods_link.t_loan_contract_info)) b
on a.serial_number = b.serial_number
group by total_overdue_daynum order by total_overdue_daynum;

-- 星连项目、机构编号下的是否有车
select
a.project_id,
a.agency_id,
if(b.id is null,'无车','有车') haveCar,
count(distinct phone_num) project_agency_haveCar_num,
sum(project_agency_num) over(partition by a.project_id,a.agency_id) project_agency_num
from (select * from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info)) a
left join (select * from ods_link.t_guaranty_car_info where spark_job_number = (select max(spark_job_number) from ods_link.t_guaranty_car_info)) b
on a.serial_number = b.serial_number
group by a.project_id,a.agency_id,haveCar
order by a.project_id,a.agency_id,haveCar;

-- 浦发数据重复原因（就是重复存储）
select a.* from (select * from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info) and project_id = 'pl00282') a
join
(select phone_num,count(phone_num) cnt from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info) and project_id = 'pl00282' group by phone_num) b
on a.phone_num = b.phone_num
where b.cnt >= 2;

-- 主要车型（或者车价范围），车龄范围，保险购买时间范围
select insurance_type,car_insurance_premium,car_sales_price,car_new_price,car_model,car_age from ods_link.t_guaranty_car_info where spark_job_number = (select max(spark_job_number) from ods_link.t_guaranty_car_info) and insurance_type is not null limit 10;
select insurance_type,car_insurance_premium,car_sales_price,car_new_price,car_model,car_age from ods_link.t_guaranty_car_info where spark_job_number = (select max(spark_job_number) from ods_link.t_guaranty_car_info) and car_sales_price is null limit 10;


-- 统计下，车型有哪些？车龄范围，按照0-1，1-3，3-5年统计下量，然后保险有的话，统计下时间近1个月，近3个月，近6个月
select
car_brand,
car_model,
case when car_age >= 0 and car_age < 1 then '0-1年'
when car_age >= 1 and car_age < 3 then '1-3年'
when car_age >= 3 and car_age < 5 then '3-5年'
else '5年以上'
end carage
from ods_link.t_guaranty_car_info where spark_job_number = (select max(spark_job_number) from ods_link.t_guaranty_car_info)
group by car_brand,car_model,carage
order by car_brand,car_model,carage;

-- 有手机的3.5万的用户群里面，top10汽车品牌是哪几个？
select
car_brand,
car_model,
count(distinct phone_num) cnt
from (select * from ods_link.t_borrower_info where spark_job_number = (select max(spark_job_number) from ods_link.t_borrower_info)) a
join
(select * from ods_link.t_guaranty_car_info where spark_job_number = (select max(spark_job_number) from ods_link.t_guaranty_car_info)) b
on a.serial_number = b.serial_number
group by car_brand,car_model
order by cnt desc
limit 10;



select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and (company_code != 10014 or company_code is null);

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10014;

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);


select count(org_code) from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);
select count(product_code) from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);
select count(client_id) from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200;




select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) < '2018-12-31';

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) < '2018-12-31' and company_code = 10014;

select count(distinct a.client_id) from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) < '2018-12-31') a
join (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200) b
on a.client_id = b.client_id;

select count(a.client_id) from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) < '2018-12-31') a
join (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200) b
on a.client_id = b.client_id;




select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) > '2018-12-31';

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) > '2018-12-31' and company_code = 10014;

select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) between '2019-01-01' and '2019-05-07';


select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) between '2018-10-26' and '2018-12-31';



select count(distinct a.client_id) from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) > '2018-12-31' and company_code = 10014) a
join (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200) b
on a.client_id = b.client_id;

select count(a.client_id) from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and to_date(apply_date) > '2018-12-31' and company_code = 10014) a
join (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200) b
on a.client_id = b.client_id;



select count(client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and replace(to_date(client_info.apply_date),'-','') between '20190101' and '20190507';

select count(client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and (company_code != 10014 or company_code is null) and replace(to_date(client_info.apply_date),'-','') between '20190101' and '20190507';

select count(client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and (company_code != 10014 or company_code is null) and replace(to_date(client_info.apply_date),'-','') between '20190101' and '20190507';

-- 帮我查一下从4月1号到昨天 点点有效率总共多少个 然后乘以12
-- 点点的所有产品  点点金融和点点
-- 再查点点所有产品的 1月18到3月底的有效量
select
product_name,
count(distinct client_id)*12
from
(select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date between '20190118' and '20190301' and recommend_ret = 200) a
join
(select * from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and product_name in ('点点','点点金融')) b
on a.product_code = b.product_code
group by product_name;


select distinct product_name,product_code,org_code from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and org_code = 10020;

select distinct org_name,org_code from ods_source.org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);


select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and product_code = 300050;


select distinct client_id from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and age > 18 and expectation > 3 and city = '上海' and (have_car > 0 or have_house > 0 or insure > 0 or accumulationfund > 0) and apply_date between '2019-05-16 16:00:00' and '2019-05-16 16:52:00' order by client_id;


select count(distinct client_id) from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and date = '20190515';


select accumulationfund,count(accumulationfund) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) group by accumulationfund;


select count(distinct client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and apply_date between '2019-05-16 16:00:00' and '2019-05-16 16:52:00';


-- 6—8看下有没有上海并且3万以上并且公积金/房/车/微粒贷有一个有
select distinct client_id from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and expectation > 3 and city = '上海' and (have_car > 0 or have_house > 0 or accumulationfund > 0 or WEILI > 1) and apply_date between '2019-05-23 18:00:00' and '2019-05-23 20:00:00' order by client_id;




-- 新浪17-23号的0-7点的进件转化情况(有效推送比有效接收)
-- 查看流量信息表，找出新浪的编号      10032 新浪爱问
select org_code,org_name from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);
-- 新浪的有效接收数
select
if(substring(apply_date,12,2)<'07',concat('有效接收 ',to_date(apply_date),' < 07'),concat('有效接收 ',to_date(apply_date),' > 07')) datetime,
org_name,
count(distinct client_id)
from (select * from ods_source.client_info where spark_job_number = 28438 and company_code = '10032' and to_date(apply_date) between '2019-05-17' and '2019-05-23') client_info
left join ods_source.source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = 28438)
group by datetime,org_name
order by datetime;
-- 商品推送与有效推送
select
if(substring(apply_date,12,2)<'07',concat(to_date(apply_date),' < 07'),concat(to_date(apply_date),' > 07')) datetime,
org_name,
product_name,
if(recommend_ret = 200,'成功推送','问题推送') ret,
count(recommend_recon.client_id) person_push
from (select * from ods_source.client_info where spark_job_number = 28438 and company_code = '10032' and to_date(apply_date) between '2019-05-17' and '2019-05-23') client_info
join ods_source.source_org_info on (client_info.company_code = source_org_info.org_code and source_org_info.spark_job_number = 28438)
join ods_source.recommend_recon on (client_info.client_id = recommend_recon.client_id and recommend_recon.spark_job_number = 28438 and recommend_recon.date between '20190517' and '20190523')
join ods_source.product_info on (recommend_recon.product_code = product_info.product_code and product_info.spark_job_number = 28438)
group by datetime,org_name,product_name,ret
order by datetime,product_name,ret;


-- 线上推送，有效推送数 融泽财富 钞急好借 点点贷 点点金融 百度有钱花 聚财小神牛 代你还 信用超人 信用超人贷
-- 线下推送，有效推送数 51银行贷 帮帮优贷 东方融资网 佳佳融 未睐贷款 信诺贷 融贷通 助贷网 晟振舒贷 贷融融 房融通 御顺金融
-- 线上推送数(即有效推送数)
select org_code,org_name from ods_source.source_org_info where spark_job_number = 30366;-- 10032

select
date,
product_name,
count(recommend_recon.client_id)
from (select * from ods_source.recommend_recon where spark_job_number = 30366 and date = '20190527') recommend_recon
join (select * from ods_source.product_info where spark_job_number = 30366) product_info on recommend_recon.product_code = product_info.product_code
join (select * from ods_source.client_info where spark_job_number = 30366 and company_code = 10032) client_info on recommend_recon.client_id = client_info.client_id
where product_info.product_name in ("融泽财富","钞急好借","点点贷","点点金融","百度有钱花","聚财小神牛","代你还","信用超人","信用超人贷")
group by date,product_name
order by date,product_name;
-- 线下推送数
select
date,
product_name,
count(recommend_recon.client_id)
from (select * from ods_source.recommend_recon where spark_job_number = 30366 and date = '20190527') recommend_recon
join (select * from ods_source.product_info where spark_job_number = 30366) product_info on recommend_recon.product_code = product_info.product_code
join (select * from ods_source.client_info where spark_job_number = 30366 and company_code = 10032) client_info on recommend_recon.client_id = client_info.client_id
where product_info.product_name in ("51银行贷","帮帮优贷","东方融资网","佳佳融","未睐贷款","信诺贷","融贷通","助贷网","晟振舒贷","贷融融","房融通","御顺金融")
group by date,product_name
order by date,product_name;
-- 线下有效推送数
select
date,
product_name,
count(recommend_recon.client_id)
from (select * from ods_source.recommend_recon where spark_job_number = 30366 and date = '20190527' and recommend_ret = 200) recommend_recon
join (select * from ods_source.product_info where spark_job_number = 30366) product_info on recommend_recon.product_code = product_info.product_code
join (select * from ods_source.client_info where spark_job_number = 30366 and company_code = 10032) client_info on recommend_recon.client_id = client_info.client_id
where product_info.product_name in ("51银行贷","帮帮优贷","东方融资网","佳佳融","未睐贷款","信诺贷","融贷通","助贷网","晟振舒贷","贷融融","房融通","御顺金融")
group by date,product_name
order by date,product_name;


-- 因特力1和新浪爱问28号0~7点的独家/非独家有效接收数和不同产品方的有效推送数
set day=29;
select
concat(a.datetime,' 0~7点') datetime,
a.org_name,
a.route,
product_name,
pull_route,
pull,
push_product,
push_route,
push
from
(select
  to_date(apply_date) datetime,
  org_name,
  route,
  count(distinct client_info.client_id) pull_route,
  sum(count(distinct client_info.client_id)) over(partition by to_date(apply_date),org_name) pull
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10001 and apply_date between '2019-05-${day} 00:00:00' and '2019-05-${day} 07:00:00') client_info
  left join (select * from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) source_org_info on client_info.company_code = source_org_info.org_code
  group by datetime,org_name,route) a
left join
(select
  to_date(apply_date) datetime,
  org_name,
  route,
  product_name,
  count(distinct client_info.client_id) push_product,
  sum(count(distinct client_info.client_id)) over(partition by to_date(apply_date),org_name,route) push_route,
  sum(count(distinct client_info.client_id)) over(partition by to_date(apply_date),org_name) push
  from (select * from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and company_code = 10001 and apply_date between '2019-05-${day} 00:00:00' and '2019-05-${day} 07:00:00') client_info
  left join (select * from ods_source.source_org_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) source_org_info on client_info.company_code = source_org_info.org_code
  join (select * from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.client_info) and recommend_ret = 200 and date = '201905${day}') recommend_recon on client_info.client_id = recommend_recon.client_id
  left join (select * from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info)) product_info on recommend_recon.product_code = product_info.product_code
  group by datetime,org_name,route,product_name) b
on a.datetime = b.datetime and a.org_name = b.org_name and a.route = b.route
order by datetime,org_name,route,product_name;


-- 新星源有效推送，有效接收
select
substring(client_info.ctime,0,8) datetime,
source_info.name source_name,
city,
expectation,
count(distinct client_info.id) sum_expectation,
sum(count(distinct client_info.id)) over(partition by substring(client_info.ctime,0,8),source_info.name,city) sum_city,
sum(count(distinct client_info.id)) over(partition by substring(client_info.ctime,0,8),source_info.name) sum_source
from (select * from ods_source_new.client_info where substring(ctime,0,8) = 20190529) client_info
join (select * from ods_source_new.source_info) source_info on client_info.sourcechannel = source_info.id
group by datetime,source_name,city,expectation
order by datetime,source_name,city,expectation;

select
substring(client_info.ctime,0,8) datetime,
source_info.name source_name,
city,
expectation,
count(distinct client_info.id) sum_expectation,
sum(count(distinct client_info.id)) over(partition by substring(client_info.ctime,0,8),source_info.name,city) sum_city,
sum(count(distinct client_info.id)) over(partition by substring(client_info.ctime,0,8),source_info.name) sum_source
from (select * from ods_source_new.client_info where substring(ctime,0,8) = 20190529) client_info
join (select * from ods_source_new.source_info) source_info on client_info.sourcechannel = source_info.id
join (select * from ods_source_new.recommend_flow) recommend_flow on client_info.id = recommend_flow.client_id
group by datetime,source_name,city,expectation
order by datetime,source_name,city,expectation;


-- 产品方去重有效期(300027 闪电借款)
select distinct product_code,product_name,asserts_expire from ods_source.product_info where spark_job_number = (select max(spark_job_number) from ods_source.product_info);

-- 有空帮我查两个数 老星源，流水表一共有多少条记录/client info一共有多少客户  （都不去重）
select count(client_id) from ods_source.client_info where spark_job_number = (select max(spark_job_number) from ods_source.client_info);
select count(task_id) from ods_source.event_logger where spark_job_number = (select max(spark_job_number) from ods_source.event_logger);
select count(client_id) from ods_source.recommend_recon where spark_job_number = (select max(spark_job_number) from ods_source.recommend_recon);


-- 25-55岁,期望金额3万以上,公积金(已缴纳或满三年)、工作发放形式(现金或银行代发)、房产>0、车产>0、保险>0   age为0
select id,city,sourcechannel,expectation
from ods_source_new.client_info_csv
where sourcechannel = 5 and (ctime between '20190601000000' and '20190603000000' or utime between '20190601000000' and '20190603000000') and expectation > 1 and (wagePayment > 0 or haveHouse > 0 or haveCar > 0 or accumulationfund > 0 or insure > 0);

-- 未睐传过来时的时间点
select distinct substring(ctime,1,10) datetime
from ods_source_new.client_info_csv
where sourcechannel = 5 and (ctime between '20190601000000' and '20190603000000' or utime between '20190601000000' and '20190603000000') order by datetime;


-- 新星源有效接收推送
select
a.datetime,
a.source_name,
pull_sum,
b.product_name,
push_sum
from
(select
  substring(if(client_info.utime = '',client_info.ctime,client_info.utime),1,8) datetime,
  source_info.name source_name,
  count(client_info.id) pull_sum
  from (select * from ods_source_new.client_info) client_info
  left join ods_source_new.source_info on client_info.fchannel = s1.id = source_info.id
  group by datetime,source_name) a
left join
(select
  substring(if(client_info.utime = '',client_info.ctime,client_info.utime),1,8) datetime,
  source_info.name source_name,
  product_info.name product_name,
  count(client_info.id) push_sum
  from (select * from ods_source_new.client_info) client_info
  left join ods_source_new.source_info on client_info.fchannel = s1.id = source_info.id
  left join (select * from ods_source_new.recommend_flow) recommend_flow on client_info.id = recommend_flow.clientid
  left join ods_source_new.product_info on recommend_flow.productid = product_info.id
  group by datetime,source_name,product_name) b
on a.datetime = b.datetime and a.source_name = b.source_name
order by datetime,source_name,product_name,pull_sum;



-- 新星源需求(总计数据量)
select
count(event_logger.clientid)
from ods_source_new.event_logger
left join ods_source_new.client_info on event_logger.clientid = client_info.id
left join ods_source_new.source_info on client_info.sourcechannel = source_info.id
left join ods_source_new.recommend_flow on client_info.id = recommend_flow.clientid
left join ods_source_new.product_info on recommend_flow.productid = product_info.id
left join ods_source_new.org_info on product_info.tenantid = org_info.id;


-- 看看这些数据是那个渠道过去的
select distinct to_date(apply_date) datetime,a.mobile,a.city,b.org_name,a.source
from ods_source.client_info a
join ods_source.source_org_info b on a.company_code = b.org_code
where a.mobile in ('pfOx6pIpmmUyaH1ndS8K4A==','h24rDRP2eRwIy5/QaCSlUw==','fgqYs4e0CPfBM2Hf5IbQdw==','WxEgYO/5XUXKY5I76XUkdA==');

-- 其中10个在老系统中没有，新系统中也没有
select distinct a.ctime,a.utime,a.mobile,a.city,b.name,a.schannel,a.tchannel
from ods_source_new.client_info a
join ods_source_new.source_info b on a.sourcechannel = b.id
where a.mobile in ('pfOx6pIpmmUyaH1ndS8K4A==','h24rDRP2eRwIy5/QaCSlUw==','fgqYs4e0CPfBM2Hf5IbQdw==','WxEgYO/5XUXKY5I76XUkdA==') order by a.ctime;



-- 9、10号 期望金额1万以上,公积金(已缴纳或满三年)、房产>0、车产>0
select distinct datetime,expectation,accumulation_fund,have_house,have_car from dwd_inter.event_client_source where substring(datetime,1,8) between '20190609' and '20190609' and expectation > 1 and (accumulation_fund > 0 or have_house > 0 or have_car > 0) order by datetime;


-- 3万以上，上海，有车或有房或有公积金或有保单，25-55岁（左闭右开）,城市为 广州、上海、苏州、南京、杭州、武汉、合肥、佛山
select distinct
substring(a.datetime,1,8) datetime,
a.source_name,
a.client_id,
city,
age,
expectation,
accumulation_fund,
have_house,
have_car,
product_name,
push_code
from (select * from dwd_inter.event_client_source
  where substring(datetime,1,8) between '20190609' and '20190610' and age between '25' and '55' and expectation > 3 and (accumulation_fund > 0 or have_house > 0 or have_car > 0 or insure > 0) and city in ('广州','上海','苏州','南京','杭州','武汉','合肥','佛山')) a
left join dwd_inter.recommend_product_org b
on substring(a.datetime,1,8) = substring(b.datetime,1,8) and a.client_id = b.client_id
order by datetime,source_name,city,age,client_id;



-- 卡集10号没有推出去是为什么     都是测试数据
select distinct * from (select * from dwd_inter.event_client_source where substring(datetime,1,8) between '201906010' and '20190610' and source_name = '卡集') a
left join dwd_inter.recommend_product_org b
on substring(a.datetime,1,8) = substring(b.datetime,1,8) and a.client_id = b.client_id
order by datetime,source_name,product_name,client_id;


-- 卡集11号25笔成功推送的是否有身份证号
select
*
from (select * from ods_source_new.client_info where substring(ctime,1,8) between '20190611' and '20190611') a
join ods_source_new.recommend_flow b on a.id = b.clientid
join ods_source_new.source_info c on a.fchannel = s1.id = c.id
where c.name = '卡集' and b.code in (200,400);



-- 老星源的用户数，以及三要素信息都有的有多少人
select count(mobile) from ods_source.client_info where spark_job_number = 37130 and (name is not null and id_card is not null and mobile is not null);
select count(mobile) from ods_source.client_info where spark_job_number = 37130;


-- 11号的流量方为null
select * from ods_source_new.event_logger where substring(ctime,1,8) between '20190611' and '20190611';
INSERT INTO TABLE dwd_inter.event_client_source
select * from dwd_inter.event_client_source_old where substring(datetime,1,8) < '20190611' or substring(datetime,1,8) > '20190611' order by datetime;


-- 哪些用户推送给了融贷通
select distinct city from dwd_inter.recommend_product_org join dwd_inter.event_client_source on recommend_product_org.client_id = event_client_source.client_id where product_name = '融贷通' and substring(event_client_source.datetime,1,8) = '20190612';


-- 11——因特利 11号19 到21点推送成功数量
select
push_code,
count(distinct b.client_id)
from dwd_inter.event_client_source a join dwd_inter.recommend_product_org b on a.client_id = b.client_id
where a.source_id = 11 and a.datetime between '2019061119000000' and '2019061121000000' and b.push_code in (200,400)
group by push_code;


-- 查出这三个人的个人信息 lGosp+2h2cB3ev4+wAJITQ== e/jezzKD/YM95Q3/OMvCmw== a2tfEW9VGqUNFkD5QovJ6g==
select * from ods_source_new.client_info where mobile in ("lGosp+2h2cB3ev4+wAJITQ==","e/jezzKD/YM95Q3/OMvCmw==","a2tfEW9VGqUNFkD5QovJ6g==");

mongo 10.80.16.34:27017/starsource -u mongouser -p S6gvEdMzYVUT8x
db.getCollection('CLIENT_INFO').find({"$or":[{"mobile":"lGosp+2h2cB3ev4+wAJITQ=="},{"mobile":"e/jezzKD/YM95Q3/OMvCmw=="},{"mobile":"a2tfEW9VGqUNFkD5QovJ6g=="}]})

select fchannel = s1.id,intersectiontags from ods_source_new.client_info where (ctime between '20190501000000' and '20190621000000' or utime between '20190501000000' and '20190621000000');


-- 万丈、卡集的信息问题(城市、额度信息)可能有问题 9 卡集  26 万丈
desc ods_source_new.source_info;

select id,name from ods_source_new.source_info where name in ('万丈','卡集');

select fchannel = s1.id,id,name,idcard,mobile,birthdate,sex,province,city,expectation,age from ods_source_new.client_info where (ctime between '20190620000000' and '20190621000000' or utime between '20190620000000' and '20190621000000') and fchannel = s1.id in (9,26) order by fchannel = s1.id;

desc dwd_inter.event_client_source;

select source_name,client_id,name,id_card,mobile,birthdate,sex,province,city,expectation,age from dwd_inter.event_client_source where datetime between '20190620000000' and '20190621000000' and source_name in ('万丈','卡集');

db.getCollection('CLIENT_INFO').find({"$or":[{"fchannel = s1.id":"9"},{"fchannel = s1.id":"26"}]})

-- 针对透传时存在的期望金额为null时必是≥5的情况做了优化
sh /home/hadoop/star_source/start.sh

/home/hadoop/spark-2.3.2-bin-hadoop2.7/bin/beeline -u jdbc:hive2://10.80.176.20:10000 -n hadoop --hivevar startDate=20190101000000 --showHeader=false --silent=true --outputformat=csv2 -e 'select * from dm_cf.source_city_exp_pull' | sed '/^\s*$/d' > /home/hadoop/star_source/mysql/source_city_exp_pull.csv
/usr/bin/mysql -P3306 -h10.80.176.22 -umeta -pmeta2015 -Ddatamart -e "truncate source_city_exp_pull;load data local infile '/home/hadoop/star_source/mysql/source_city_exp_pull.csv' into table source_city_exp_pull fields terminated by ',';"


-- 查看新系统中是否存在一个手机号对应多个client_id的情况
select substring(ctime,1,8) dt,mobile,count(1) from ods_source_new.client_info group by mobile,dt having count(1) > 1 order by dt;

-- 查看老系统中是否存在一个手机号对应多个client_id的情况
select to_date(apply_date) dt,mobile,count(1) from ods_source.client_info where spark_job_number = 41952 group by dt,mobile having count(1) > 1 order by dt;
select to_date(apply_date) dt,mobile,count(1) from ods_source.client_info where spark_job_number = 41952 group by dt,mobile having count(1) > 1 order by dt;
select to_date(apply_date) dt,mobile,count(1) from ods_source.client_info where spark_job_number = 41952 group by dt,mobile having count(1) > 1 order by dt;

select * from ods_source.client_info where spark_job_number = 41952 and mobile in ('wZN4+P+zNbul4eNUGNKeDg==','esZW5rG5kNu/uwIGmtGG7g==','zwZhx4ua2rH5Pm1S5QZwzg==','joc5+1RNCD2yGTM2hlEnKQ==') order by mobile;
select * from ods_source.recommend_recon where spark_job_number = 41952 and client_id in ('5d0a7f168786b0e40df1c727','5d0a7f168786b0e40ef1c727','5d0a8e8b6204b0e422c91421','5d0a8e8b8786b0e420f1c727','d0a866d8786b0e417f1c727','5d0a86c26204b0e419c91421','5d0a8a366204b0e41ec91421','5d0a8a368786b0e41cf1c727') order by client_id;


-- dm_user_info任务挂了，查找原因(因为数据重复造成的)
select spark_job_number from ods_link.t_loan_contract_info order by spark_job_number desc limit 1; -- 19289;
select spark_job_number from int_party.int_source_org_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from int_party.int_user_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from int_party.int_asset_product_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from int_party.int_asset_org_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_link.t_guaranty_car_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_link.t_associates_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_source.ads_inbound order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_source.recommend_recon order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from dm_cf.dm_user_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from int_party.int_his_asset_basic_info order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_bank.hisassetbasicinfonew order by spark_job_number desc limit 1; -- 41952;
select spark_job_number from ods_bank.hisassetbasicinfo order by spark_job_number desc limit 1; -- 15915;


select count(1) from int_party.int_his_asset_basic_info;

select count(1),mobile from int_party.int_user_info where spark_job_number=41952 group by mobile having count(1) > 1;

select count(1),mobile from dm_cf.dm_user_info where spark_job_number=41952 group by mobile having count(1) > 1;


select count(1),mobile from ods_source.client_info where spark_job_number=40988 group by mobile having count(1) > 1;



select count(1),mobile from etl_cf.int_user_info_tmp  where spark_job_number=41952 group by mobile having count(1) > 1;

select count(1),mobile from etl_cf.int_user_info_source_tmp  where spark_job_number=41952 group by mobile having count(1) > 1;

select count(1),mobile from etl_cf.client_info_tmp  where spark_job_number=41952 group by mobile having count(1) > 1;

select count(1),cust_code from int_party.int_his_asset_basic_info where spark_job_number=41952 group by cust_code having count(1) > 1;

select count(1),product_code from int_party.int_asset_product_info where spark_job_number=41952 group by product_code having count(1) > 1;

select count(1),org_code from int_party.int_asset_org_info where spark_job_number=41952 group by org_code having count(1) > 1;

select count(1),org_code from int_party.int_source_org_info where spark_job_number=41952 group by org_code having count(1) > 1;

-- 星连
select count(1),phone_num from ods_link.t_borrower_info where spark_job_number=40988 group by phone_num having count(1) > 1;

-- 星云
select count(1),tel from ods_bank.customerinfo where spark_job_number=40988 group by tel having count(1) > 1;

select count(1),custcode from ods_bank.HISASSETBASICINFONEW where spark_job_number=41952 group by custcode having count(1) > 1;

select count(1),custcode from ods_bank.HISASSETBASICINFONEWETBASICINFO where spark_job_number=15915 group by custcode having count(1) > 1;

select
COUNT(1),
mobile
FROM
(select
  t.mobile,
  MAX(t.create_time) create_time
  FROM
  ods_source.client_info t
  where t.spark_job_number = 41952
  GROUP BY t.mobile) a
GROUP BY a.mobile
HAVING COUNT(1) > 1 ;

/*
用其中一个手机号码分别放进去查一下
ukEs5roZf4At+Ydf+nHyMw==
bEE+Qem63qIw1wp3WqQ6aA==
*/

select count(*) from ods_source.client_info where mobile='ukEs5roZf4At+Ydf+nHyMw==';
select count(*) from ods_bank.customerinfo where tel='ukEs5roZf4At+Ydf+nHyMw==';
select count(*) from ods_link.t_borrower_info where phone_num='ukEs5roZf4At+Ydf+nHyMw==';

select
COUNT(*)
FROM
(select
  b.*
  FROM
  (select
    t.mobile,
    MAX(t.create_time) create_time
    FROM
    ods_source.client_info t
    where t.spark_job_number = 41952
    GROUP BY t.mobile) a
  LEFT JOIN ods_source.client_info b
  ON a.mobile = b.mobile
  and a.create_time = b.create_time
  and b.spark_job_number = 41952
  where b.mobile IS NOT NULL) a
where a.mobile = 'bEE+Qem63qIw1wp3WqQ6aA==';

select
t.mobile,
t.create_time
FROM
ods_source.client_info t
where t.spark_job_number = 41952
and t.mobile = 'bEE+Qem63qIw1wp3WqQ6aA==' ;


select
COUNT(*)
FROM
(select
  b.*
  FROM
  (select
    t.mobile,
    MAX(t.create_time) create_time
    FROM
    ods_source.client_info t
    where t.spark_job_number = 41952
    GROUP BY t.mobile) a
  LEFT JOIN ods_source.client_info b
  ON a.mobile = b.mobile
  where a.create_time = b.create_time
  and b.spark_job_number = 41952
  and b.mobile IS NOT NULL) a
where a.mobile = 'bEE+Qem63qIw1wp3WqQ6aA==' ;

-- 广州、一万以上、有房>0或有车>0或社保>-1
select * from dwd_inter.event_client_source where datetime between '20190623000000' and '20190625000000' and city = '广州市' and expectation > 1 and (have_house > 0 or have_car > 0 or insure > -1) and mobile is not null;
select * from dwd_inter.event_client_source where datetime between '20190625000000' and '20190626000000' and city = '广州市' and expectation > 1 and (have_house > 0 or have_car > 0 or insure > -1) and mobile is not null;
select * from dwd_inter.event_client_source where datetime between '20190625000000' and '20190626000000' and city = '广州市' and expectation > 3 and (have_house > 0 or have_car > 0 or insure > -1) and mobile is not null;


-- 查看这些用户的流量方
select distinct substring(datetime,1,8) date,mobile,source_name from dwd_inter.event_client_source where mobile in ("2nDMhSrVz3aa+PjiM+u7hQ==","b2ARTaXMsJBMjcuWXLOrfw==") order by date;
select distinct to_date(apply_date) date,mobile,org_name from ods_source.client_info join ods_source.source_org_info on client_info.company_code = source_org_info.org_code where client_info.spark_job_number = (select max(spark_job_number) from ods_source.client_info) and source_org_info.spark_job_number = (select max(spark_job_number) from ods_source.source_org_info) and client_info.mobile in ("2nDMhSrVz3aa+PjiM+u7hQ==","b2ARTaXMsJBMjcuWXLOrfw==") order by date;



-- 新星源 因特利符合上海大于3W的、公积金满三年、有房、有车、保险满两年、年龄在28-55之间
select count(distinct client_id) from dwd_inter.event_client_source where source_name = '因特利' and substring(datetime,1,8) = '20190627' and city = '上海市' and expectation > 3 and (accumulation_fund > 1 or have_house > 0 or have_car > 0 or insure > 1) and age between '28' and '55';
select distinct client_id from dwd_inter.event_client_source where source_name = '因特利' and substring(datetime,1,8) = '20190627' and city = '上海市' and expectation > 3 and (accumulation_fund > 1 or have_house > 0 or have_car > 0 or insure > 1) and age between '28' and '55';
select * from dwd_inter.recommend_product_org where product_name = '房融通' and substring(datetime,1,8) = '20190627';
select distinct substring(datetime,1,10) date,client_id from dwd_inter.event_client_source_old where client_id in ('5d13b603ef6f9c2fa191fa5730407367','5d13ae6b08a4d133b253003425120740','5d139f30ef6f9c2fa191f9c981350831') order by date;
select distinct substring(datetime,1,10) date,client_id,product_name from dwd_inter.recommend_product_org_old where client_id in ('5d13b603ef6f9c2fa191fa5730407367','5d13ae6b08a4d133b253003425120740','5d139f30ef6f9c2fa191f9c981350831') order by date,product_name;
select * from dwd_inter.event_client_source where client_id in ('5d139f30ef6f9c2fa191f9c981350831');
select * from dwd_inter.recommend_product_org where client_id in ('5d139f30ef6f9c2fa191f9c981350831');

-- 报表：推荐表中的日期不允许选择  因为日期中有0000-00-00形式存在，hive数据中有null的形式
-- 5d0a9f67ef6f9c4d808c69f054239101这个client_id的source_id为空，且在client_info中未找到，但在原库中找到了，数仓数据不全
select * from dm_cf.recommend_source_product_push order by datetime;
select * from ods_source_new.recommend_flow where substring(ctime,1,8) = '20190620';
select * from ods_source_new.event_logger where clientid = '5d0cbfb6ef6f9c6c38da333a43285071' order by id,eventtype;
select * from dwd_inter.event_client_source where client_id = '5d0cbfb6ef6f9c6c38da333a43285071';
select * from ods_source_new.client_info_tmp where id = '5d0cbfb6ef6f9c6c38da333a43285071';


-- 查看13421350001这个人是哪家流量方进来的  新系统 -- 来自于即有分期
select distinct substring(datetime,1,8),source_name from dwd_inter.event_client_source where substring(datetime,1,8) = '20190704' and mobile = 'wHTSTHAP4zh/y+TvTQR2Sg==';
-- 要即有分期的所有测试数据，共两条
select distinct substring(datetime,1,8),source_name,age,sex,expectation,province,city,have_house,have_car,wagePayment,accumulation_fund,insure,credit_card,wei_li_dai from dwd_inter.event_client_source where substring(datetime,1,8) = '20190704' and source_name = '即有分期';


select distinct datetime,client_id,source_name from dwd_inter.event_client_source where expectation > 3 and event_type = 7 and substring(datetime,1,8) = '20190704' order by datetime;






select
a.date,
a.source_name,
if(push_sum is null,0,push_sum) push_sum,
if(push_eff is null,0,push_eff) push_eff,
if(push_source_sum is null,0,push_source_sum) push_source_sum,
if(push_source_eff is null,0,push_source_eff) push_source_eff,
case c.product_mode when 1 then 'H5' when 2 then 'H5' when 3 then 'API' when 4 then 'API' else 'none' end product_mode,
c.product_name,
if(push_product_sum is null,0,push_product_sum) push_product_sum,
if(push_product_eff is null,0,push_product_eff) push_product_eff
from (
  select
  substring(datetime,1,8) date,
  source_name,
  count(distinct client_id) push_sum
  from dwd_inter.recommend_product_org
  group by date,source_name
) a
left join (
  select
  substring(datetime,1,8) date,
  source_name,
  count(distinct client_id) push_eff
  from dwd_inter.recommend_product_org
  where push_code in (200,400)
  group by date,source_name
) b
on a.date = b.date and a.source_name = b.source_name
right join (
  select
  substring(datetime,1,8) date,
  source_name,
  sum(count(distinct client_id)) over(partition by substring(datetime,1,8),source_name) push_source_sum,
  product_mode,
  product_name,
  count(distinct recommend_product_org.client_id) push_product_sum
  from dwd_inter.recommend_product_org
  group by date,source_name,product_name,product_mode
) c
on a.date = c.date and a.source_name = c.source_name
left join (
  select
  substring(datetime,1,8) date,
  source_name,
  sum(count(distinct client_id)) over(partition by substring(datetime,1,8),source_name) push_source_eff,
  product_mode,
  product_name,
  count(distinct recommend_product_org.client_id) push_product_eff
  from dwd_inter.recommend_product_org
  where push_code in (200,400)
  group by date,source_name,product_name,product_mode
) d
on c.date = d.date and c.source_name = d.source_name and c.product_name = d.product_name
order by date,source_name,product_mode,product_name

-- 因特利、即有分期分发给秒速贷的量有多少
select count(distinct client_id),source_name,product_name from dwd_inter.recommend_product_org where source_name in ('因特利','即有分期') and product_name = '秒速贷' and datetime between '20190709000000' and '20190709160000' group by source_name,product_name order by source_name,product_name;

select count(distinct client_id),source_name from dwd_inter.recommend_product_org where source_name in ('因特利','即有分期')  and datetime between '20190709000000' and '20190709160000' group by source_name order by source_name;


-- 15915429927:lNOn+RrCos02PYZ0A2j73g== 是否进入 0709号未进入，0618号进来的
-- 13145921583:+TBbo29etdUvbAs/KxeLIQ==
select * from dwd_inter.event_client_source where mobile = 'lNOn+RrCos02PYZ0A2j73g==' order by datetime;
select * from dwd_inter.event_client_source where mobile = '+TBbo29etdUvbAs/KxeLIQ==' order by datetime;




-- 是hql可以执行笛卡尔积
set spark.sql.crossJoin.enabled=true;

-- 查找为什么两张表的同一项会有两个值
-- 两种查询方式的对比
-- 第一种
select
substring(datetime,1,8) date,source_name,
sum(count(distinct client_id)) over(partition by substring(datetime,1,8),source_name) pull_source,
if(city is null,null,city) city,
sum(count(distinct client_id)) over(partition by substring(datetime,1,8),source_name,city) pull_city,
case when 0 < expectation and expectation < 0.1 then 'lt_point_1'
when 0.1 <= expectation and expectation < 0.5 then 'lt_point_5'
when 0.5 <= expectation and expectation < 1 then 'lt_1'
when 1 <= expectation and expectation < 3 then 'lt_3'
when 3 <= expectation and expectation < 5 then 'lt_5'
when expectation = 0 then 'zero'
when expectation is null then 'empty'
else 'gte_5' end exp_level,
count(distinct client_id) pull_exp
from dwd_inter.event_client_source
where event_type >1 and substr(datetime,1,8) = '20190709'
group by date,source_name,city,exp_level
order by date,source_name,city,exp_level;
-- 第二种
select
substring(datetime,1,8) date,
source_name,
sum(count(distinct client_id)) over(partition by substring(datetime,1,8),source_name) pull_sum,
city,
count(distinct client_id) pull_eff
from dwd_inter.event_client_source
where event_type >1  and substr(datetime,1,8) = '20190709'  and source_name = '即有分期'
group by date,source_name,city
order by date,source_name,city;
-- 查找出原因为expectation作了修改，但是被重复统计
select distinct client_id,expectation,source_name,city from dwd_inter.event_client_source where event_type >1  and substr(datetime,1,8) = '20190709' distribute by client_id,expectation,source_name,city limit 10;


-- 信用转转到5点和6点时，分别有多少量15号
select
product_name,
count(distinct client_id)
from dwd_inter.recommend_product_org
where product_name = '信用转转' and push_code in (200,400) and '2019071500' <= substring(datetime,1,10) and substring(datetime,1,10) < '2019071516'
group by product_name;


-- fchannel = s1.id=44的，16号16点以前的，推荐结果为200的总数  44是
select distinct source_name from dwd_inter.event_client_source where source_id = '44';
select count(distinct client_id),source_name from dwd_inter.recommend_product_org where source_name = '匹尔斯' and push_code = '200' and '2019071600' <= substring(datetime,1,10) and substring(datetime,1,10) < '2019071616' group by source_name;

-- 树袋熊的推送成功数
select count(distinct client_id),source_name,product_name from dwd_inter.recommend_product_org where source_name = '未睐' and product_name = '树袋熊' and push_code = '200' and '20190716' = substring(datetime,1,8) group by source_name,product_name;



-- 16号推给秒速贷的推送有多少
select count(distinct client_id),source_name,product_name from dwd_inter.recommend_product_org where product_name = '秒速贷' and substring(datetime,1,8) = '20190716' and push_code = '200' group by source_name,product_name;



-- 0720 新浪爱问 --> 御顺金融、达州市的client_id
select distinct recommend_product_org.client_id,recommend_product_org.source_name,city,product_name,recommend_product_org.datetime,event_client_source.datetime,event_client_source.source_name from dwd_inter.event_client_source join dwd_inter.recommend_product_org on (event_client_source.client_id = recommend_product_org.client_id and event_client_source.source_name = recommend_product_org.source_name) where recommend_product_org.source_name = '新浪爱问' and product_name = '御顺金融' and substring(recommend_product_org.datetime,1,8) = '20190720';


-- 新星源现有人数
select count(distinct client_id) from dwd_inter.event_client_source;
select count(distinct client_id) from dwd_inter.recommend_product_org;
select count(distinct id) from ods_source_new.client_info_tmp;
select count(distinct clientid) from ods_source_new.event_logger_tmp;
select count(distinct id) from ods_source_new.client_info;


-- 万丈一次传了60笔数据过来0723
select datetime,source_name,event_type from dwd_inter.event_client_source where source_name = '万丈' and datetime between '20190723000000' and '20190724000000' and event_type >= '4' order by datetime;


-- 六月各流量方给到产品方的数量
select substring(datetime,1,6) datetime,source_name,product_name,count(distinct client_id) from dwd_inter.recommend_product_org where substring(datetime,1,6) = '201906' group by substring(datetime,1,6),source_name,product_name order by datetime,source_name,product_name;

-- 24号小B端的是一个人吗
select * from dwd_inter.recommend_product_org where substring(datetime,1,8) = '20190724' and source_name = '小B端资源';

-- 24号备点钱数据与后台对不上
select substring(datetime,1,8) datetime,source_name,count(distinct client_id) from dwd_inter.event_client_source where substring(datetime,1,8) = '20190724' and source_name = '备点钱' group by substring(datetime,1,8),source_name;


-- 查找城市为空或null
select * from dwd_inter.event_client_source right join dwd_inter.recommend_product_org on event_client_source.client_id = recommend_product_org.client_id and event_client_source.source_name = recommend_product_org.source_name where substring(event_client_source.datetime,1,8) = '20190725' and event_client_source.source_name = 'EDM直邮' and product_name = '未睐贷款';
select distinct client_id,source_name,product_name,datetime,push_code from dwd_inter.recommend_product_org where substring(datetime,1,8) = '20190618' and source_name = '未睐' and product_name is null and push_code = 200;
select distinct client_id,source_name,datetime from dwd_inter.event_client_source where substring(datetime,1,8) = '20190725' and source_name = '备点钱';


-- 24号因特利(11)，订单号没有对应的
select distinct outerorderid,clientid from ods_source_new.flow_record_tmp join ods_source_new.client_info on flow_record_tmp.clientid = client_info.id where fchannel = s1.id = 11 and substring(client_info.ctime,1,8) = 20190724;


-- 周五新浪爱问流量是不是集中过来
select substring(datetime,1,10) date_time,source_name,count(distinct client_id) from dwd_inter.event_client_source where source_name = '新浪爱问' and substring(datetime,1,8) = '20190726' group by substring(datetime,1,10),source_name order by date_time,source_name;


-- 小B端29号的手机号，看是都是新增用户
select distinct datetime,mobile,client_id,city from dwd_inter.event_client_source where substring(datetime,1,8) = '20190731' and source_name = '小B端资源';


-- 查出百G金服0704-0710的数据，给出手机、姓名的md5数据
select distinct recommend_product_org.datetime,name,mobile,product_name from dwd_inter.event_client_source right join dwd_inter.recommend_product_org on event_client_source.client_id = recommend_product_org.client_id and event_client_source.source_name = recommend_product_org.source_name where substring(recommend_product_org.datetime,1,8) between '20190704' and '20190710' and product_name = '百G金服';



-- 奢分期推给信用转转19号的推送数与推送成功数不一致
with base as
( select distinct
  substring(recommend_flow.ctime,1,8) datetime,
  recommend_flow.clientid client_id,
  product_info.name product_name,
  recommend_flow.code push_code
  from
  ods_source.recommend_flow
  left join
  ods_source.client_info
  on recommend_flow.clientid = client_info.id
  left join ods_source.source_info
  on client_info.fchannel = s1.id = source_info.id
  left join
  ods_source.product_info
  on recommend_flow.productid = product_info.id
  where substring(recommend_flow.ctime,1,8) = '20190819' and source_info.name = '奢分期' and product_info.name = '  信用转转'
)
select distinct client_id from base where client_id not in (select client_id from base where push_code in (200,400))



select count(distinct clientid),substring(recommend_flow.ctime,1,8),code,product_info.name from ods_source.recommend_flow left join ods_source.product_info on recommend_flow.productid = product_info.id where clientid in ('5d54108608a4d1539d61ea9d64998003','5d5a7f7908a4d1512aa1a7e344760578','5d5a806c08a4d1512aa1a7f288284161','5d5a868f08a4d1512aa1a80584559727','5d5aa439ef6f9c72c74de88a16949050','5d55d1a1ef6f9c701011f2ff45370474','5d5a84c8ef6f9c72c74de86862556558','5d5a929908a4d1512aa1a81042705395','5d5aafc708a4d1512aa1a83037767724','5d5ac506ef6f9c72c74de8b438492870') group by substring(recommend_flow.ctime,1,8),code,product_info.name;


select distinct if(s1.name is null,source_info.name,concat(source_info.name,'~~',split(s1.name,'-')[0]))
from ods_source.flow_record
left join ods_source.source_info on flow_record.fchannel = source_info.id
left join ods_source.source_info s1 on flow_record.schannel = s1.id
where flow_record.mobile = '+LClarH30n0BHleoeeJgZg==';

-- 查询手机号是哪个流量方的
select distinct
flow_record.mobile,
if(s1.name is null,source_info.name,concat(source_info.name,'~~',split(s1.name,'-')[0]))
from ods_source.flow_record
left join ods_source.source_info on flow_record.fchannel = source_info.id
left join ods_source.source_info s1 on flow_record.schannel = s1.id
join ods_source.recommend_flow on flow_record.id = recommend_flow.worderid
left join ods_source.product_info on recommend_flow.productid = product_info.id and product_info.name = '御顺金融'
where flow_record.mobile in ();



-- 小B细分渠道的UV和PV数15-23号
with base_table as
( select
  substring(event_logger.ctime,1,8) datetime,
  event_logger.eventtype event_type,
  count(distinct clientid) event_count_cid,
  if(eventtype = 1,count(distinct orderid),0) event_count_oid,
  s1.name f_name,
  s2.name s_name,
  s3.name t_name
  from
  ods_source.event_logger
  left join
  ods_source.client_info
  on event_logger.clientid = client_info.id
  left join
  ods_source.source_info as s1
  on split(event_logger.sourcechannel,"~~")[0] = s1.id or split(event_logger.sourcechannel,"-")[0] = s1.id
  left join
  ods_source.source_info as s2
  on split(event_logger.sourcechannel,"~~")[1] = s2.id or split(event_logger.sourcechannel,"-")[1] = s2.id
  left join
  ods_source.source_info as s3
  on split(event_logger.sourcechannel,"~~")[2] = s3.id or split(event_logger.sourcechannel,"-")[2] = s3.id
  where eventtype in (1,2,4,7) and substring(event_logger.ctime,1,8) between "20190815" and "20190823" and s1.name = "小B端资源"
  group by substring(event_logger.ctime,1,8),s1.name,s2.name,s3.name,event_logger.eventtype
)
select
datetime,
f_name,
s_name,
t_name,
case event_type
when 0 then "PV数量"
when 1 then "UV数量"
when 2 then "注册数量"
when 4 then "留资数量"
when 7 then "推送数量"
end event_type,
event_count
from
( select datetime,f_name,s_name,t_name,event_type,event_count_cid event_count from base_table union all
  select datetime,f_name,s_name,t_name,0 event_type,event_count_oid event_count from base_table where event_type = 1
) a
order by datetime,f_name,s_name,t_name,event_count desc,event_type;


-- 城市分发倍数报表
with base as
( select
  substring(recommend_flow.utime,1,8) as datetime,
  source_info.name as source_name,
  client_info.city as city,
  product_info.name as product_name,
  recommend.clientid as re_clientid,
  recommend_flow.clientid as rf_clientid
  from
  ods_source.recommend_flow
  left join
  ods_source.recommend_flow as recommend
  on recommend_flow.id = recommend.id and recommend.code = 200
  left join
  ods_source.product_info
  on recommend_flow.productid = product_info.id
  left join
  ods_source.client_info
  on recommend_flow.clientid = client_info.id
  left join
  ods_source.source_info
  on client_info.fchannel = s1.id = source_info.id
  where product_info.mode = 4 and substring(recommend_flow.utime,1,8) between "20190815" and "20190815"
)
select
a.datetime,
a.source_name,
b.city,
sum(count_recommend_200)/count_source_200 as succe_eff,
sum(count_recommend_full)/count_source_full as total_eff
from
( select
  datetime,
  source_name,
  count(distinct re_clientid) as count_source_200,
  count(distinct rf_clientid) as count_source_full
  from base
  group by datetime,source_name
) as a
join
( select
  datetime,
  source_name,
  city,
  count(distinct re_clientid) as count_recommend_200,
  count(distinct rf_clientid) as count_recommend_full
  from base
  group by datetime,source_name,city,product_name
) as b
on a.datetime = b.datetime and a.source_name = b.source_name
group by a.datetime,a.source_name,b.city,count_source_200,count_source_full
order by datetime,source_name,city;




startDate=20190101

endDate=20190905
unset beeline
beeline="$beeline_home $hive2_url $user"
beeline="$beeline --hivevar startDate=$startDate --hivevar endDate=$endDate"


-- 小B端报表
select
if(t_ad_query_water.app.id is null,
  if(t_ad_query_water.site.name is null,t_ad_query_water.site.id,t_ad_query_water.site.name),
  if(t_ad_query_water.app.name is null,t_ad_query_water.app.id,t_ad_query_water.app.name)
) as req_name,
count(t_ad_query_water.id) as req_count,
count(t_ad_query_water.createtime) as dvs_count,
sum(t_ad_action_water.display) as ips_count,
sum(t_ad_action_water.isclick) as clk_count
from
ods_lowerb.t_ad_query_water
left join
ods_lowerb.t_ad_action_water
on t_ad_query_water.id = t_ad_action_water.waterid
where substring(t_ad_query_water.createtime,1,8) = '20190830' or substring(t_ad_query_water.reqtime,1,8) = '20190830'
group by t_ad_query_water.app.id,t_ad_query_water.site.name,t_ad_query_water.site.id,t_ad_query_water.app.name
limit 10;



with base as
( select
  if(substring(t_ad_query_water.reqtime,1,8) is null,substring(t_ad_query_water.createtime,1,8),substring(t_ad_query_water.reqtime,1,8)) as datetime,
  t_ad_query_water.id as req_id,
  if(t_ad_query_water.createtime is null,'request','query') as req_type,
  if(t_ad_query_water.app.id is null,
    if(t_ad_query_water.site.name is null,t_ad_query_water.site.id,t_ad_query_water.site.name),
    if(t_ad_query_water.app.name is null,t_ad_query_water.app.id,t_ad_query_water.app.name)
    ) as req_name,
  t_ad_action_water.display,
  t_ad_action_water.isclick
  from
  ods_lowerb.t_ad_query_water
  left join
  ( select
    substring(createtime,1,8) as datetime,
    waterid,
    sum(display) as display,
    sum(isclick) as isclick
    from
    ods_lowerb.t_ad_action_water
    group by substring(createtime,1,8),waterid
    ) as t_ad_action_water
  on t_ad_query_water.id = t_ad_action_water.waterid and t_ad_query_water.datetime = t_ad_action_water.datetime
  where substring(t_ad_query_water.createtime,1,8) = '20190830' or substring(t_ad_query_water.reqtime,1,8) = '20190830'
)
select * from base limit 10;



select if(reqtime is null,substring(createtime,1,8),substring(reqtime,1,8)) as datetime,id,if(createtime is null,'request','query') as req_type,if(app.id is null,if(site.name is null,site.id,site.name),if(app.name is null,app.id,app.name)) as req_name from ods_lowerb.t_ad_query_water limit 500;






select * from ods_lowerb.t_ad_query_water where reqtime is not null limit 10;

select reqtime,createtime,tagid,`time` from ods_lowerb.t_ad_query_water where tagid is not null limit 10;

select `time` from ods_lowerb.t_ad_query_water where `time` is not null limit 10;

select * from ods_lowerb.t_ad_action_water limit 10;
















set hivevar:year_month=201911;
set hivevar:day_of_month=07;

INSERT OVERWRITE TABLE dm_cf.unfraud_recommend_wefix PARTITION(year_month,day_of_month)
select
if(atd_black.report_date is null,if(atd_device.report_date is null,atd_ip.report_date,atd_device.report_date),atd_black.report_date) as report_date,
if(atd_black.appid is null,if(atd_device.appid is null,atd_ip.appid,atd_device.appid),atd_black.appid) as appid,
blacklist,request_sum,
device_exce,device_good,device_gene,device_diff,device_erro,
iprate_exce,iprate_gene,iprate_diff,iprate_erro,
'${year_month}','${day_of_month}'
from
( select
  substring(`time`,0,8) as report_date,
  appid,
  count(if(inblacklist = true,id,null)) as blacklist,
  count(id) as request_sum
  from ods_wefix.atd_black_json
  where year_month = ${year_month} and day_of_month = ${day_of_month}
  group by substring(`time`,0,8),appid
) as atd_black
full join
( select
  report_date,
  appid,
  sum(device_exce) as device_exce,
  sum(device_good) as device_good,
  sum(device_gene) as device_gene,
  sum(device_diff) as device_diff,
  sum(device_erro) as device_erro
  from
  ( select
    report_date,
    appid,
    case quality when '优' then quality_count_device else 0 end device_exce,
    case quality when '良' then quality_count_device else 0 end device_good,
    case quality when '一般' then quality_count_device else 0 end device_gene,
    case quality when '差' then quality_count_device else 0 end device_diff,
    case when quality is null then quality_count_device else 0 end device_erro
    from
    ( select
      substring(`time`,0,8) as report_date,
      appid,
      if(quality not in ('优','良','一般','差'),null,quality) as quality,
      count(id) as quality_count_device
      from ods_wefix.atd_device_json
      where year_month = ${year_month} and day_of_month = ${day_of_month}
      group by substring(`time`,0,8),appid,quality
      ) as tmp
    ) as tmp
  group by report_date,appid
) as atd_device
on atd_black.report_date = atd_device.report_date and atd_black.appid = atd_device.appid
full join
( select
  report_date,
  appid,
  sum(iprate_exce) as iprate_exce,
  sum(iprate_gene) as iprate_gene,
  sum(iprate_diff) as iprate_diff,
  sum(iprate_erro) as iprate_erro
  from
  ( select
    report_date,
    appid,
    case quality when '正常' then quality_count_ip else 0 end iprate_exce,
    case quality when '一般' then quality_count_ip else 0 end iprate_gene,
    case quality when '可疑' then quality_count_ip else 0 end iprate_diff,
    case when quality is null then quality_count_ip else 0 end iprate_erro
    from
    ( select
      substring(`time`,0,8) as report_date,
      appid,
      if(quality not in ('正常','一般','可疑'),null,quality) as quality,
      count(ip) as quality_count_ip
      from ods_wefix.atd_ip_json
      where year_month = ${year_month} and day_of_month = ${day_of_month}
      group by substring(`time`,0,8),appid,quality
      ) as tmp
    ) as tmp
  group by report_date,appid
) as atd_ip
on atd_black.report_date = atd_ip.report_date and atd_black.appid = atd_ip.appid
order by report_date,appid;




-- KwjP7kvsxPKisBgHVyBxdQ 趣看天下 jJSoGLTkyryMnbw2YjLrB 微米浏览器

set hivevar:year_month=201912;
set hivevar:day_of_month=06;

with base as (
  select
  query_water.id as id,
  substring(if(query_water.reqtime is null,query_water.createtime,query_water.reqtime),1,8) as report_date,
  if(query_water.reqtime is null,'action','query') as req_type,
  -- query_water.exchange_status as exchange_status,
  action_water.createtime as action_ctime,
  query_water.acquisitionid as plan_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.apply_app_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.audit_app_id
  else null end as plan_app_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.apply_user_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.audit_user_id
  else null end as plan_user_id,
  query_water.tagid as adv_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.audit_app_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.apply_app_id
  else if(adv_info.app_id is null,null,adv_info.app_id) end as adv_app_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.audit_user_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.apply_user_id
  else if(app_info.user_id is null,null,app_info.user_id) end as adv_user_id,
  adv_info.ad_type as ad_type,
  action_water.display as action_display,
  action_water.isclick as action_isclick
  from (
    select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month
    from ods_wefix.t_ad_query_water_json
    -- where year_month = '201912' and day_of_month = '06' and (test = 0 or test is null)
    -- and (reqtime between '20191206120000' and '20191206130000' or createtime between '20191206120000' and '20191206130000')
    where year_month = '${year_month}' and day_of_month = '${day_of_month}' and (test = 0 or test is null)
    ) as query_water
  left join (
    select distinct waterid,createtime,display,isclick,year_month,day_of_month from ods_wefix.t_ad_action_water_json
    where status = 0
    ) as action_water
  on query_water.id = action_water.waterid
  left join (
    select distinct exchange_id,
    audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
    apply_adver_id,apply_plan_id,apply_app_id,apply_user_id from ods_wefix.exchange_info_tsv
    where audit_app_id != 'NULL' and audit_user_id != 'NULL' and apply_app_id != 'NULL' and apply_user_id != 'NULL' and status > 6
    ) as exchange_info
  on (query_water.tagid = exchange_info.audit_adver_id and query_water.extagid = exchange_info.apply_adver_id and query_water.acquisitionid = exchange_info.apply_plan_id)
  or (query_water.tagid = exchange_info.apply_adver_id and query_water.extagid = exchange_info.audit_adver_id and query_water.acquisitionid = exchange_info.audit_plan_id)
  left join (
    select distinct advertise_id,app_id,ad_type from ods_wefix.advertisement_info_tsv
    ) as adv_info
  on query_water.tagid = adv_info.advertise_id
  left join (
    select distinct app_id,user_id from ods_wefix.APP_INFO_tsv
    ) as app_info
  on adv_info.app_id = app_info.app_id
)
-- INSERT OVERWRITE TABLE dm_cf.advertising_space PARTITION(year_month,day_of_month)
select
-- date_format(from_utc_timestamp(current_timestamp,'GMT+8'),'yyyyMMddHHmmss') as create_date,
report_date,
plan_user_id,
plan_app_id,
plan_id,
adv_user_id,
adv_app_id,
adv_id,
ad_type,
adv_req_num,
adv_iss_num,
cast(if(adv_iss_num = 0,0,adv_iss_num/adv_req_num) as decimal(13,5)) as iss_req_rate,
adv_show_num,
cast(if(adv_show_num = 0,0,adv_show_num/adv_iss_num) as decimal(13,5)) as show_iss_rate,
adv_cli_num,
cast(if(adv_cli_num = 0,0,adv_cli_num/adv_show_num) as decimal(13,5)) as cli_show_rate
-- ,'${year_month}','${day_of_month}'
-- ,'201912','01'
from (
  select
  req.report_date as report_date,
  if(adv_req_num is null,0,adv_req_num) as adv_req_num,
  if(adv_iss_num is null,0,adv_iss_num) as adv_iss_num,
  if(adv_show_num is null,0,adv_show_num) as adv_show_num,
  if(adv_cli_num is null,0,adv_cli_num) as adv_cli_num,
  req.plan_user_id as plan_user_id,
  req.plan_app_id as plan_app_id,
  req.plan_id as plan_id,
  req.adv_user_id as adv_user_id,
  req.adv_app_id as adv_app_id,
  req.adv_id as adv_id,
  req.ad_type as ad_type
  from (
    select report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type,count(distinct id) as adv_req_num
    from base
    group by report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type
    ) as req
  left join (
    select report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type,count(distinct id) as adv_iss_num
    from base where req_type = 'action'
    group by report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type
    ) as iss
  on req.report_date = iss.report_date and req.plan_user_id = iss.plan_user_id and req.plan_id = iss.plan_id and req.adv_user_id = iss.adv_user_id and req.adv_app_id = iss.adv_app_id and req.adv_id = iss.adv_id and req.ad_type = iss.ad_type and req.plan_app_id = iss.plan_app_id
  left join (
    select report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type,
    count(distinct if(action_display = 1,id,null)) as adv_show_num,
    count(distinct if(action_isclick = 1,id,null)) as adv_cli_num
    from base where req_type = 'action'
    group by report_date,plan_user_id,plan_app_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type
    ) as show_cli
  on req.report_date = show_cli.report_date and req.plan_user_id = show_cli.plan_user_id and req.plan_id = show_cli.plan_id and req.adv_user_id = show_cli.adv_user_id and req.adv_app_id = show_cli.adv_app_id and req.adv_id = show_cli.adv_id and req.ad_type = show_cli.ad_type and req.plan_app_id = show_cli.plan_app_id
) as a
order by report_date,plan_user_id,plan_app_id,plan_user_id,plan_id,adv_user_id,adv_app_id,adv_id,ad_type;



select distinct
advertisement_info_tsv.advertise_id,
advertisement_info_tsv.advertise_name,
advertisement_info_tsv.app_id,
app_info_tsv.app_name,
advertisement_info_tsv.shielding_industry
from ods_wefix.advertisement_info_tsv
left join ods_wefix.app_info_tsv
on advertisement_info_tsv.app_id = app_info_tsv.app_id
where advertise_id in ('CV6Pn5ApiHT6K9j5bNjQAW','HKHfWCcbzpDs7cKqgrYJmz')
;




-- 861843037934370 超时，864854041429916 未上报
select
query_water.tagid,query_water.acquisitionid,query_water.extagid,
-- sum(action_water.display)
action_water.display
-- query_water.device.id,action_water.status
from (
  select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month,device
  from ods_wefix.t_ad_query_water_json
  where year_month = '201912' and day_of_month = '04'
  and (test = 0 or test is null)
) as query_water
left join (
  select distinct waterid,createtime,display,isclick,status,year_month,day_of_month from ods_wefix.t_ad_action_water_json
) as action_water
on query_water.id = action_water.waterid
-- where query_water.tagid = 'KwjP7kvsxPKisBgHVyBxdQ'
where query_water.tagid = 'Bxg8EjYEAfbDMdNhvAByzn'
and query_water.extagid = '2tMveHpPfG9bbpB4Q2gbRq'
-- where query_water.tagid = 'Bxg8EjYEAfbDMdNhvAByzn'
-- and query_water.acquisitionid = 408
-- and query_water.acquisitionid = 421
-- 此为查出所有关于 KwjP7kvsxPKisBgHVyBxdQ 这个广告位的IMEI码
-- and query_water.createtime is not null
-- 此为查出所有上报的关于 KwjP7kvsxPKisBgHVyBxdQ 这个广告位的IMEI码
and action_water.display = 1
-- 此为查出所有除超时5秒上报外的所有有效数据
-- and status = 0
-- group by query_water.tagid,query_water.acquisitionid,query_water.extagid
;







select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month,device
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '05'
and (test = 0 or test is null)
and reqtime is null and createtime is null
;



select
*
from
( select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month
  from ods_wefix.t_ad_query_water_json
  where year_month = '201912' and day_of_month = '11'
  and tagid = '83RNDbxnUMR83sxo7dzixt'
) as query_water
left join (
  select distinct advertise_id,app_id,ad_type from ods_wefix.advertisement_info_tsv
) as adv_info
on query_water.tagid = adv_info.advertise_id
left join (
  select distinct app_id,user_id from ods_wefix.APP_INFO_tsv
) as app_info
on adv_info.app_id = app_info.app_id
limit 50
;

select distinct advertise_id,app_id,ad_type
from ods_wefix.advertisement_info_tsv
where advertise_id = '83RNDbxnUMR83sxo7dzixt'
;


select distinct exchange_id,
audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
from ods_wefix.exchange_info_tsv
where (audit_adver_id = 'Bxg8EjYEAfbDMdNhvAByzn' or apply_adver_id = 'Bxg8EjYEAfbDMdNhvAByzn')
and audit_adver_id != 'NULL' and audit_plan_id != 'NULL' and audit_app_id != 'NULL' and audit_user_id != 'NULL'
and apply_adver_id != 'NULL' and apply_plan_id != 'NULL' and apply_app_id != 'NULL' and apply_user_id != 'NULL'
-- and status > 6
;



select distinct exchange_id,
audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
apply_adver_id,apply_plan_id,apply_app_id,apply_user_id from ods_wefix.exchange_info_tsv
where audit_app_id != 'NULL' and audit_user_id != 'NULL' and apply_app_id != 'NULL' and apply_user_id != 'NULL'
and status > 6
;


select distinct exchange_id,
audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
from ods_wefix.exchange_info_tsv
where audit_adver_id != 'NULL' and audit_plan_id != 'NULL' and audit_app_id != 'NULL' and audit_user_id != 'NULL'
and apply_adver_id != 'NULL' and apply_plan_id != 'NULL' and apply_app_id != 'NULL' and apply_user_id != 'NULL'
and (audit_adver_id = 'MMiQ2q1KkJud8ZBU34uLo5' and apply_plan_id = 416 and apply_adver_id = '2tMveHpPfG9bbpB4Q2gbRq')
or (apply_adver_id = 'MMiQ2q1KkJud8ZBU34uLo5' and audit_plan_id = 416 and audit_adver_id = '2tMveHpPfG9bbpB4Q2gbRq')
-- exchange_id = 84
;



select distinct
-- extagid
id,reqtime,createtime,tagid,acquisitionid,extagid,test
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '17' and (test = 0 or test is null)
-- and tagid = 'MMiQ2q1KkJud8ZBU34uLo5'
and tagid = '77xgucLfh9e2L64sW3KUq1'
-- and (test = 0 or test is null)
and test != 1
-- and extagid = '2tMveHpPfG9bbpB4Q2gbRq'
-- and acquisitionid = 416
and acquisitionid = 359
limit 5
;


set hivevar:num_sub=8;
select
-- *
count(distinct id) as total,
substring(reqtime,1,${num_sub}) as reqtime,
substring(createtime,1,${num_sub}) as createtime,
tagid,acquisitionid,extagid,year_month,day_of_month
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '11' and (test = 0 or test is null)
and tagid = '83RNDbxnUMR83sxo7dzixt'
-- and extagid = 'Bxg8EjYEAfbDMdNhvAByzn'
group by substring(reqtime,1,${num_sub}),substring(createtime,1,${num_sub}),tagid,acquisitionid,extagid,year_month,day_of_month
having total > 25
order by total desc,reqtime,createtime
limit 50
;


select
-- distinct waterid,
count(distinct waterid),
-- sum(display) as display,
-- sum(isclick) as isclick,
sourceId,extagid from ods_wefix.t_ad_action_water_json
-- where sourceId = 'CdbaYKjoNtaAXCh53BbN9B' and extagid = 'Bxg8EjYEAfbDMdNhvAByzn'
where sourceId = '24LF3C2YfmCXFmerY4w74P' and extagid = '2tMveHpPfG9bbpB4Q2gbRq'
and year_month = '201912' and day_of_month = '04'
-- and createtime > '20191204190000'
and status = 0
-- and display = 1
and isclick = 1
-- and waterid not in (
--   select
--   distinct action_water.waterid
--   -- query_water.tagid,query_water.acquisitionid,query_water.extagid,
--   -- sum(action_water.display)
--   -- action_water.display
--   -- query_water.device.id,action_water.status
--   from (
--     select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month,device
--     from ods_wefix.t_ad_query_water_json
--     where year_month = '201912' and day_of_month = '04'
--     and (test = 0 or test is null)
--   ) as query_water
--   left join (
--     select distinct waterid,createtime,display,isclick,status,year_month,day_of_month from ods_wefix.t_ad_action_water_json
--   ) as action_water
--   on query_water.id = action_water.waterid
--   -- where query_water.tagid = 'KwjP7kvsxPKisBgHVyBxdQ'
--   where query_water.tagid = 'Bxg8EjYEAfbDMdNhvAByzn'
--   and query_water.extagid = '2tMveHpPfG9bbpB4Q2gbRq'
--   -- where query_water.tagid = 'Bxg8EjYEAfbDMdNhvAByzn'
--   -- and query_water.acquisitionid = 408
--   -- and query_water.acquisitionid = 421
--   -- 此为查出所有关于 KwjP7kvsxPKisBgHVyBxdQ 这个广告位的IMEI码
--   -- and query_water.createtime is not null
--   -- 此为查出所有上报的关于 KwjP7kvsxPKisBgHVyBxdQ 这个广告位的IMEI码
--   and action_water.display = 1
--   -- 此为查出所有除超时5秒上报外的所有有效数据
--   -- and status = 0
--   -- group by query_water.tagid,query_water.acquisitionid,query_water.extagid
-- )
group by sourceId,extagid
;




select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month,device
from ods_wefix.t_ad_query_water_json
where id = 84853763
;


DROP TABLE IF EXISTS dm_cf.advertising_space_tmp;
-- 只能用于读取
CREATE EXTERNAL TABLE IF NOT EXISTS dm_cf.advertising_space_tmp (
  `create_date`   string  COMMENT '插入日期',
  `report_date`   string  COMMENT '报告日期',
  `plan_user_id`  string  COMMENT '获客计划对应的用户id',
  `plan_app_id`   string  COMMENT '获客计划对应的AppID',
  `plan_id`       bigint  COMMENT '获客计划ID',
  `adv_user_id`   string  COMMENT '广告位对应的用户id',
  `adv_app_id`    string  COMMENT '广告位对应的AppID',
  `adv_id`        string  COMMENT '广告位ID',
  `adv_type`      int     COMMENT '广告位类型',
  `adv_req_num`   int     COMMENT '广告请求数',
  `adv_iss_num`   int     COMMENT '广告下发数',
  `iss_req_rate`  double  COMMENT '下发数与请求数比值',
  `adv_show_num`  int     COMMENT '广告展示数',
  `show_iss_rate` double  COMMENT '展示数与下发数比值',
  `adv_cli_num`   int     COMMENT '广告点击数',
  `cli_show_rate` double  COMMENT '点击数与展示数比值'
) COMMENT 'WeFix置换交易数据概览'
-- PARTITIONED BY(year_month string COMMENT '年月',day_of_month string COMMENT '天')
-- STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JDBCStorageHandler'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.83.16.14/microb",
  "hive.sql.table"          = "ADVERTISING_SPACE",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Xfx2018@)!*",
  "hive.sql.dbcp.maxActive" = "1"

  -- "mapred.jdbc.driver.class"      = "com.mysql.jdbc.Driver",
  -- "mapred.jdbc.url"               = "jdbc:mysql://localhost:3306/rstore",
  -- "mapred.jdbc.username"          = "root",
  -- "mapred.jdbc.password"          = "Xfx2018@)!*",
  -- "mapred.jdbc.input.table.name"  = "JDBCTable",
  -- "mapred.jdbc.output.table.name" = "JDBCTable",
  -- "mapred.jdbc.hive.lazy.split"   = "false"
);

select * from dm_cf.advertising_space_tmp;










ALTER TABLE ods_wefix.atd_black_json ADD IF NOT EXISTS PARTITION (year_month='201911',day_of_month='29');
ALTER TABLE ods_wefix.atd_device_json ADD IF NOT EXISTS PARTITION (year_month='201911',day_of_month='29');
ALTER TABLE ods_wefix.atd_ip_json ADD IF NOT EXISTS PARTITION (year_month='201911',day_of_month='29');






show partitions dm_cf.advertising_space;


show partitions dm_cf.unfraud_recommend_wefix;

show partitions ods_wefix.atd_black_json;
show partitions ods_wefix.atd_device_json;
show partitions ods_wefix.atd_ip_json;

set hivevar:year_month=201912;
set hivevar:day_of_month=12;

-- select * from ods_wefix.atd_black_json where year_month = ${year_month} and day_of_month = ${day_of_month} limit 10;
-- select * from ods_wefix.atd_device_json where year_month = ${year_month} and day_of_month = ${day_of_month} limit 10;
-- select * from ods_wefix.atd_ip_json where year_month = ${year_month} and day_of_month = ${day_of_month} limit 10;

ALTER TABLE ods_wefix.atd_black_json ADD IF NOT EXISTS PARTITION (year_month='${year_month}',day_of_month='${day_of_month}');
ALTER TABLE ods_wefix.atd_device_json ADD IF NOT EXISTS PARTITION (year_month='${year_month}',day_of_month='${day_of_month}');
ALTER TABLE ods_wefix.atd_ip_json ADD IF NOT EXISTS PARTITION (year_month='${year_month}',day_of_month='${day_of_month}');

-- INSERT OVERWRITE TABLE dm_cf.unfraud_recommend_wefix PARTITION(year_month,day_of_month)
select
if(atd_black.report_date is null,if(atd_device.report_date is null,atd_ip.report_date,atd_device.report_date),atd_black.report_date) as report_date,
if(atd_black.appid is null,if(atd_device.appid is null,atd_ip.appid,atd_device.appid),atd_black.appid) as appid,
blacklist,request_sum,
device_exce,device_good,device_gene,device_diff,device_erro,
iprate_exce,iprate_gene,iprate_diff,iprate_erro
-- ,'${year_month}','${day_of_month}'
from
( select
  substring(`time`,0,8) as report_date,
  appid,
  count(if(inblacklist = true,id,null)) as blacklist,
  count(id) as request_sum
  from ods_wefix.atd_black_json
  where year_month = ${year_month} and day_of_month = ${day_of_month}
  -- where year_month = 201911 and day_of_month = 28
  -- and `time` between '20191206120000' and '20191206130000'
  group by substring(`time`,0,8),appid
  -- order by report_date;
) as atd_black
full join
( select
  report_date,
  appid,
  sum(device_exce) as device_exce,
  sum(device_good) as device_good,
  sum(device_gene) as device_gene,
  sum(device_diff) as device_diff,
  sum(device_erro) as device_erro
  from
  ( select
    report_date,
    appid,
    case quality when '优' then quality_count_device else 0 end device_exce,
    case quality when '良' then quality_count_device else 0 end device_good,
    case quality when '一般' then quality_count_device else 0 end device_gene,
    case quality when '差' then quality_count_device else 0 end device_diff,
    case when quality is null then quality_count_device else 0 end device_erro
    from
    ( select
      substring(`time`,0,8) as report_date,
      appid,
      if(quality not in ('优','良','一般','差'),null,quality) as quality,
      count(id) as quality_count_device
      from ods_wefix.atd_device_json
      where year_month = ${year_month} and day_of_month = ${day_of_month}
      -- where year_month = 201912 and day_of_month = 06 and `time` between '20191206120000' and '20191206130000'
      group by substring(`time`,0,8),appid,quality
      ) as tmp
    ) as tmp
  group by report_date,appid
  -- order by report_date;
) as atd_device
on atd_black.report_date = atd_device.report_date and atd_black.appid = atd_device.appid
full join
( select
  report_date,
  appid,
  sum(iprate_exce) as iprate_exce,
  sum(iprate_gene) as iprate_gene,
  sum(iprate_diff) as iprate_diff,
  sum(iprate_erro) as iprate_erro
  from
  ( select
    report_date,
    appid,
    case quality when '正常' then quality_count_ip else 0 end iprate_exce,
    case quality when '一般' then quality_count_ip else 0 end iprate_gene,
    case quality when '可疑' then quality_count_ip else 0 end iprate_diff,
    case when quality is null then quality_count_ip else 0 end iprate_erro
    from
    ( select
      substring(`time`,0,8) as report_date,
      appid,
      if(quality not in ('正常','一般','可疑'),null,quality) as quality,
      count(ip) as quality_count_ip
      from ods_wefix.atd_ip_json
      where year_month = ${year_month} and day_of_month = ${day_of_month}
      -- where year_month = 201912 and day_of_month = 06 and `time` between '20191206120000' and '20191206130000'
      group by substring(`time`,0,8),appid,quality
      ) as tmp
    ) as tmp
  group by report_date,appid
  -- order by report_date;
) as atd_ip
on atd_black.report_date = atd_ip.report_date and atd_black.appid = atd_ip.appid
order by report_date,appid;




select report_date,app_id,blacklist,request_sum,device_exce,device_good,device_gene,device_diff,device_erro,iprate_exce,iprate_gene,iprate_diff,iprate_erro,year_month,day_of_month
from dm_cf.unfraud_recommend_wefix
-- where report_date > 20191127
-- where year_month = 201901 and day_of_month = 01
order by report_date
;


desc ods_wefix.atd_black_json;
desc ods_wefix.atd_device_json;
desc ods_wefix.atd_ip_json;







set hivevar:year_month=201912;
set hivevar:day_of_month=12;

set hivevar:s_num=9;
set hivevar:e_num=2;
with base as (
  select id,reqtime,createtime,tagid,acquisitionid,extagid,display,isclick,if(app_name is null,tagid,app_name) as app_name
  from (
    select distinct id,reqtime,createtime,tagid,acquisitionid,extagid
    from ods_wefix.t_ad_query_water_json
    where year_month = '${year_month}' and day_of_month = '${day_of_month}'
    and (test = 0 or test is null)
  -- and code = '20028'
  ) as query_water
  left join (
    select distinct waterid,display,isclick
    from ods_wefix.t_ad_action_water_json
    where status = 0
    ) as action_water
  on query_water.id = action_water.waterid
  left join (
    select distinct advertise_id,app_id from ods_wefix.advertisement_info_tsv
    ) as adv_info
  on query_water.tagid = adv_info.advertise_id
  left join (
    select distinct app_id,app_name from ods_wefix.APP_INFO_tsv
    ) as app_info
  on adv_info.app_id = app_info.app_id
)
select
req_table.report_date as report_date,
if(req_sum is null,0,req_sum) as req_sum,
if(iss_sum is null,0,iss_sum) as iss_sum,
if(show_num is null,0,show_num) as show_num,
if(cli_num is null,0,cli_num) as cli_num,
req_table.app_name as app_name
from (
  select substring(if(reqtime is null,createtime,reqtime),${s_num},${e_num}) as report_date,app_name,
  count(distinct if(reqtime is not null,id,null)) as req_sum,
  count(distinct if(createtime is not null,id,null)) as iss_sum
  from base
  group by substring(if(reqtime is null,createtime,reqtime),${s_num},${e_num}),app_name
) as req_table
left join (
  select substring(createtime,${s_num},${e_num}) as report_date,app_name,
  count(distinct if(display = 1,id,null)) as show_num,
  count(distinct if(isclick = 1,id,null)) as cli_num
  from base where createtime is not null
  group by substring(createtime,${s_num},${e_num}),app_name
) as s_c_table
on req_table.report_date = s_c_table.report_date and req_table.app_name = s_c_table.app_name
order by app_name,report_date
-- limit 50
;

select from ods_wefix.atd_device_json
select from ods_wefix.atd_ip_json


select
distinct type,id
-- count(distinct id) as cnt
from ods_wefix.atd_black_json
where type = 'imei'
and length(id) != 15 and length(id) != 16 and length(id) != 17
-- order by id
limit 50
;


set hivevar:s_num=9;
set hivevar:e_num=2;







set hivevar:num=2;
select
max(substring(dt,1,8)) as max_dt,
datediff(
  from_unixtime(unix_timestamp(max(substring(dt,1,8)),'yyyyMMdd'),'yyyy-MM-dd'),
  from_unixtime(unix_timestamp(min(substring(dt,1,8)),'yyyyMMdd'),'yyyy-MM-dd')
) + 1 as dt_diff,
count(distinct substring(dt,1,8)) as dt_cnt,
min(substring(dt,1,8)) as min_dt,
substring(dt,9,${num}) as hour,
max(req_cnt) as req_max,
min(req_cnt) as req_min,
cast(sum(req_cnt)/count(distinct substring(dt,1,8)) as decimal(13,2)) as req_avg,
sum(req_cnt) as req_cnt,
app_name
from (
  select count(distinct id) as req_cnt,dt,if(app_name is null,tagid,app_name) as app_name
  from (select distinct id,substring(if(reqtime is null,createtime,reqtime),1,${num} + 8) as dt,tagid
    from ods_wefix.t_ad_query_water_json
    where (test = 0 or test is null)
    -- and year_month = '${year_month}' and day_of_month = '${day_of_month}'
    ) as query_water
  left join (select distinct advertise_id,app_id from ods_wefix.advertisement_info_tsv) as adv_info
  on query_water.tagid = adv_info.advertise_id
  left join (select distinct app_id,app_name from ods_wefix.APP_INFO_tsv) as app_info
  on adv_info.app_id = app_info.app_id
  group by dt,if(app_name is null,tagid,app_name)

) as base
where app_name not in ('k135-a57','kn-a57','test_tagId','wefixt1','wefixtb')
and dt < from_unixtime(unix_timestamp(),'yyyyMMdd')
group by app_name,substring(dt,9,${num})
order by app_name,hour
;











set hivevar:num=2;
with base as (
  select *
  from (
    select count(distinct id) as req_cnt,dt,if(app_name is null,tagid,app_name) as app_name
    from (select distinct id,substring(if(reqtime is null,createtime,reqtime),1,${num} + 8) as dt,tagid
      from ods_wefix.t_ad_query_water_json
      where (test = 0 or test is null)
    -- and year_month = '${year_month}' and day_of_month = '${day_of_month}'
    ) as query_water
    left join (select distinct advertise_id,app_id from ods_wefix.advertisement_info_tsv) as adv_info
    on query_water.tagid = adv_info.advertise_id
    left join (select distinct app_id,app_name from ods_wefix.APP_INFO_tsv) as app_info
    on adv_info.app_id = app_info.app_id
    group by dt,if(app_name is null,tagid,app_name)
    ) as base
  where app_name not in ('k135-a57','kn-a57','test_tagId','wefixt1','wefixtb')
  and dt < from_unixtime(unix_timestamp(),'yyyyMMdd')
)
select
max_dt,
dt_diff,
dt_cnt,
min_dt,
base_dt.hour as hour,
req_max,
req_min,
cast(req_avg as decimal(13,2)) as req_avg,
cast(stan_dev as decimal(13,2)) as stan_dev,
req_cnt,
base_dt.app_name as app_name
from
(
  select
  max(substring(dt,1,8)) as max_dt,
  datediff(
    from_unixtime(unix_timestamp(max(substring(dt,1,8)),'yyyyMMdd'),'yyyy-MM-dd'),
    from_unixtime(unix_timestamp(min(substring(dt,1,8)),'yyyyMMdd'),'yyyy-MM-dd')
    ) + 1 as dt_diff,
  count(distinct substring(dt,1,8)) as dt_cnt,
  min(substring(dt,1,8)) as min_dt,
  substring(dt,9,${num}) as hour,
  max(req_cnt) as req_max,
  min(req_cnt) as req_min,
  sum(req_cnt)/count(distinct substring(dt,1,8)) as req_avg,
  sum(req_cnt) as req_cnt,
  app_name
  from base
  group by app_name,substring(dt,9,${num})
) as base_dt
left join
(
  select distinct
  substring(dt,9,${num}) as hour,
  stddev(req_cnt) over(partition by app_name,substring(dt,9,${num})) as stan_dev,
  app_name
  from base
  order by app_name,hour
) as base_sd
on base_dt.hour = base_sd.hour and base_dt.app_name = base_sd.app_name
order by app_name,hour
;






select waterId from ods_wefix.atd_black_json where substring(`time`,1,8) = 20191215
limit 50
;

select waterId from ods_wefix.atd_ip_json where substring(`time`,1,8) = 20191215 and waterid not in (select waterId from ods_wefix.atd_black_json where substring(`time`,1,8) = 20191215)
limit 50
;



show partitions dm_cf.advertising_space;
show partitions ods_wefix.t_ad_query_water_json;


select report_date,app_name,blacklist,request_sum,device_exce,device_good,device_gene,device_diff,device_erro,iprate_exce,iprate_gene,iprate_diff,iprate_erro,year_month,day_of_month
from dm_cf.unfraud_recommend_wefix
-- where year_month = 201911 and day_of_month = 07
order by report_date,app_name
;

show partitions ods_wefix.t_ad_action_water_json;
show partitions ods_wefix.t_ad_query_water_json;



ALTER TABLE ods_wefix.t_ad_query_water_json DROP IF EXISTS PARTITION (year_month = '20191',day_of_month = '21');
ALTER TABLE ods_wefix.t_ad_query_water_json DROP IF EXISTS PARTITION (year_month = '20191',day_of_month = '29');
ALTER TABLE ods_wefix.t_ad_action_water_json DROP IF EXISTS PARTITION (year_month = '20191',day_of_month = '21');
ALTER TABLE ods_wefix.t_ad_action_water_json DROP IF EXISTS PARTITION (year_month = '20191',day_of_month = '29');

select distinct exchange_id,
audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
from ods_wefix.exchange_info_tsv
where (audit_user_id = 'microb_super_admin' and apply_plan_id = 500)
or (apply_user_id = 'microb_super_admin' and audit_plan_id = 500)
;


select id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month
from ods_wefix.t_ad_query_water_json
where year_month = 201911 and day_of_month = 21
;


select distinct advertise_id,user_id,app_name,create_time,update_time
from (
  select distinct * from ods_wefix.advertisement_info_tsv
) as adv_info
left join (
  select distinct app_id,app_name,user_id from ods_wefix.app_info_tsv
) as app_info
on adv_info.app_id = app_info.app_id
where adv_info.advertise_id = 'LeaTD6CBQ29Zz2msWgKMNn'
;



select distinct acquisition_id,user_id from ods_wefix.acquisition_plan_tsv where acquisition_id = 359
;

select distinct
id,reqtime,createtime,tagid,acquisitionid,extagid,test
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '17' and test != 1
and tagid = '77xgucLfh9e2L64sW3KUq1'
and acquisitionid = 359
limit 5
;



select distinct
id,reqtime,createtime,tagid,acquisitionid,extagid,test
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '18'
-- and test != 1
-- and (test = 0 or test is null)
limit 5
;





-- 20191219 10752697  imei  966809422913919 2JdX3WKytiiD7ziwpTVz2e  163 1 true  0 false
select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,fLevel,fStatus,inBlackList from ods_wefix.atd_black_json
-- where year_month = 201912 and day_of_month = 19
where waterid = 10752697
limit 50
;

-- 20191219 10752697  imei  966809422913919 2JdX3WKytiiD7ziwpTVz2e  163 1 差 0 NULL
select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,fLevel,fStatus,quality from ods_wefix.atd_device_json
-- where year_month = 201912 and day_of_month = 19
where waterid = 10752697
limit 50
;

select substring(`time`,0,8) as report_date,waterid,ip,appid,exid,status,fLevel,fStatus,quality from ods_wefix.atd_ip_json
-- where year_month = 201912 and day_of_month = 19
-- where ip = '116.25.189.6'
where waterid = 10752697
limit 50
;










set hivevar:year_month=201912;
set hivevar:day_of_month=23;

with base as (
  select
  report_date,
  waterid,
  if(exchange_info.apply_app_id = appid,exchange_info.audit_user_id,exchange_info.apply_user_id)    as  login_userid,
  if(exchange_info.apply_app_id = appid,exchange_info.audit_app_id,exchange_info.apply_app_id)      as  login_appid,
  if(exchange_info.apply_app_id = appid,exchange_info.audit_adver_id,exchange_info.apply_adver_id)  as  login_advid,
  if(exchange_info.apply_app_id = appid,exchange_info.apply_app_id,exchange_info.audit_app_id)      as  viewer_appid,
  if(exchange_info.apply_app_id = appid,exchange_info.apply_adver_id,exchange_info.audit_adver_id)  as  viewer_advid,
  device_type,
  device_id,
  status_b,
  flevel_b,
  fstatus_b,
  inblacklist,
  status_d,
  flevel_d,
  fstatus_d,
  quality_d,
  status_i,
  flevel_i,
  fstatus_i,
  ip,
  quality_i
  from
  ( select
    if(atd_black.report_date is null,if(atd_device.report_date is null,atd_ip.report_date,atd_device.report_date),atd_black.report_date) as report_date,
    if(atd_black.waterid is null,if(atd_device.waterid is null,atd_ip.waterid,atd_device.waterid),atd_black.waterid) as waterid,
    if(atd_black.appid is null,if(atd_device.appid is null,atd_ip.appid,atd_device.appid),atd_black.appid) as appid,
    if(atd_black.exid is null,if(atd_device.exid is null,atd_ip.exid,atd_device.exid),atd_black.exid) as exid,
    if(atd_black.type is null,atd_device.type,atd_black.type) as device_type,
    if(atd_black.id is null,atd_device.id,atd_black.id) as device_id,
    atd_black.status        as  status_b,
    atd_black.flevel        as  flevel_b,
    atd_black.fstatus       as  fstatus_b,
    atd_black.inblacklist   as  inblacklist,
    atd_device.status       as  status_d,
    atd_device.flevel       as  flevel_d,
    atd_device.fstatus      as  fstatus_d,
    atd_device.quality      as  quality_d,
    atd_ip.status           as  status_i,
    atd_ip.flevel           as  flevel_i,
    atd_ip.fstatus          as  fstatus_i,
    atd_ip.ip               as  ip,
    atd_ip.quality          as  quality_i
    from
    ( select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,flevel,fstatus,inblacklist
      from ods_wefix.atd_black_json
      -- where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_black
    full join
    ( select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,flevel,fstatus,quality
      from ods_wefix.atd_device_json
      -- where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_device on atd_black.waterid = atd_device.waterid and atd_black.appid = atd_device.appid and atd_black.exid = atd_device.exid
    full join
    ( select substring(`time`,0,8) as report_date,waterid,ip,appid,exid,status,flevel,fstatus,quality
      from ods_wefix.atd_ip_json
      -- where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_ip on (atd_black.waterid = atd_ip.waterid and atd_black.appid = atd_ip.appid and atd_black.exid = atd_ip.exid)
    or (atd_device.waterid = atd_ip.waterid and atd_device.appid = atd_ip.appid and atd_device.exid = atd_ip.exid)
    ) as adt
  left join (select distinct exchange_id,
    audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
    from ods_wefix.exchange_info_tsv
    where audit_app_id != 'null' and audit_user_id != 'null' and apply_app_id != 'null' and apply_user_id != 'null' and status > 6
    ) as exchange_info on adt.exid = exchange_info.exchange_id
)
-- INSERT OVERWRITE TABLE dm_cf.adt_data PARTITION(year_month,day_of_month)
select
report_date,
login_userid,
login_appid,
login_advid,
viewer_appid,
viewer_advid,
count(waterid) as  req_sum,
sum(if(fstatus_b = 1 and fstatus_d != 1 and fstatus_i != 1,1,0))  as  blacklist_sum,
sum(if(fstatus_b != 1 and fstatus_d = 1 and fstatus_i != 1,1,0))  as  sus_device_sum,
sum(if(fstatus_b != 1 and fstatus_d != 1 and fstatus_i = 1,1,0))  as  sus_ip_sum,
sum(if(fstatus_b = 1 and fstatus_d = 1 and fstatus_i != 1,1,0))   as  bl_device_sum,
sum(if(fstatus_b = 1 and fstatus_d != 1 and fstatus_i = 1,1,0))   as  bl_ip_sum,
sum(if(fstatus_b != 1 and fstatus_d = 1 and fstatus_i = 1,1,0))   as  dvi_ip_sum,
sum(if(fstatus_b = 1 and fstatus_d = 1 and fstatus_i = 1,1,0))    as  bl_dvi_ip_sum
  -- ,'${year_month}','${day_of_month}'
  from base
  where fstatus_d = 1
  group by report_date,login_userid,login_appid,login_advid,viewer_appid,viewer_advid;





-- 查询ADT中相差数据的信息
select waterid from ods_wefix.atd_black_json where year_month = 201912 and day_of_month = 23 and appid = '24LF3C2YfmCXFmerY4w74P'
and waterid not IN (select waterid from ods_wefix.atd_device_json where year_month = 201912 and day_of_month = 23 and appid = '24LF3C2YfmCXFmerY4w74P');

select waterid from ods_wefix.atd_device_json where year_month = 201912 and day_of_month = 23 and appid = '24LF3C2YfmCXFmerY4w74P';

select waterid from ods_wefix.atd_ip_json where year_month = 201912 and day_of_month = 23 and appid = '24LF3C2YfmCXFmerY4w74P' and waterid not IN (select waterid from ods_wefix.atd_device_json where year_month = 201912 and day_of_month = 23 and appid = '24LF3C2YfmCXFmerY4w74P');


select fLevel,fStatus,inBlackList,count(waterId) from ods_wefix.atd_black_json group by fLevel,fStatus,inBlackList;

select fLevel,fStatus,quality,count(waterId) from ods_wefix.atd_device_json group by fLevel,fStatus,quality;

select fLevel,fStatus,quality,count(waterId) from ods_wefix.atd_ip_json group by fLevel,fStatus,quality;



select min(substring(`time`,1,8)) from ods_wefix.atd_black_json where exid is not null;










set hivevar:year_month=201912;

set hivevar:day_of_month=26;

with base as (
  select
  report_date,
  waterid,
  if(exchange_info.apply_app_id = appid,app_audit.app_name,app_apply.app_name)  as  login_appname,
  if(exchange_info.apply_app_id = appid,adv_audit.adv_name,adv_apply.adv_name)  as  login_advname,
  if(exchange_info.apply_app_id = appid,app_apply.app_name,app_audit.app_name)  as  viewer_appname,
  if(exchange_info.apply_app_id = appid,adv_apply.adv_name,adv_audit.adv_name)  as  viewer_advname,
  device_type,
  device_id,
  status_b,
  flevel_b,
  fstatus_b,
  inblacklist,
  status_d,
  flevel_d,
  fstatus_d,
  quality_d,
  status_i,
  flevel_i,
  fstatus_i,
  ip,
  quality_i
  from
  ( select
    if(atd_black.report_date is null,if(atd_device.report_date is null,atd_ip.report_date,atd_device.report_date),atd_black.report_date) as report_date,
    if(atd_black.waterid is null,if(atd_device.waterid is null,atd_ip.waterid,atd_device.waterid),atd_black.waterid) as waterid,
    if(atd_black.appid is null,if(atd_device.appid is null,atd_ip.appid,atd_device.appid),atd_black.appid) as appid,
    if(atd_black.exid is null,if(atd_device.exid is null,atd_ip.exid,atd_device.exid),atd_black.exid) as exid,
    if(atd_black.type is null,atd_device.type,atd_black.type) as device_type,
    if(atd_black.id is null,atd_device.id,atd_black.id) as device_id,
    atd_black.status        as  status_b,
    atd_black.flevel        as  flevel_b,
    atd_black.fstatus       as  fstatus_b,
    atd_black.inblacklist   as  inblacklist,
    atd_device.status       as  status_d,
    atd_device.flevel       as  flevel_d,
    atd_device.fstatus      as  fstatus_d,
    atd_device.quality      as  quality_d,
    atd_ip.status           as  status_i,
    atd_ip.flevel           as  flevel_i,
    atd_ip.fstatus          as  fstatus_i,
    atd_ip.ip               as  ip,
    atd_ip.quality          as  quality_i
    from
    ( select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,flevel,fstatus,inblacklist
      from ods_wefix.atd_black_json
      where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_black
    full join
    ( select substring(`time`,0,8) as report_date,waterid,type,id,appid,exid,status,flevel,fstatus,quality
      from ods_wefix.atd_device_json
      where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_device on atd_black.waterid = atd_device.waterid and atd_black.appid = atd_device.appid and atd_black.exid = atd_device.exid
    full join
    ( select substring(`time`,0,8) as report_date,waterid,ip,appid,exid,status,flevel,fstatus,quality
      from ods_wefix.atd_ip_json
      where year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as atd_ip on (atd_black.waterid = atd_ip.waterid and atd_black.appid = atd_ip.appid and atd_black.exid = atd_ip.exid)
    or (atd_device.waterid = atd_ip.waterid and atd_device.appid = atd_ip.appid and atd_device.exid = atd_ip.exid)
    ) as adt
  left join (select distinct exchange_id,audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
    from ods_wefix.exchange_info_tsv
    where audit_app_id != 'null' and audit_user_id != 'null' and apply_app_id != 'null' and apply_user_id != 'null' and status > 6
    ) as exchange_info on adt.exid = exchange_info.exchange_id
  left join (select distinct app_id,app_name from ods_wefix.app_info_tsv) as app_audit on exchange_info.audit_app_id = app_audit.app_id
  left join (select distinct app_id,app_name from ods_wefix.app_info_tsv) as app_apply on exchange_info.apply_app_id = app_apply.app_id
  left join (select distinct advertise_id,advertise_name as adv_name from ods_wefix.advertisement_info_tsv) as adv_audit on exchange_info.audit_adver_id = adv_audit.advertise_id
  left join (select distinct advertise_id,advertise_name as adv_name from ods_wefix.advertisement_info_tsv) as adv_apply on exchange_info.apply_adver_id = adv_apply.advertise_id
)
-- INSERT OVERWRITE TABLE dm_cf.adt_admin PARTITION(year_month,day_of_month)
select distinct
report_date,
login_appname,
login_advname,
viewer_appname,
viewer_advname,
status_b,
flevel_b,
fstatus_b,
inblacklist,
count(if(fstatus_b is null,null,waterid)) over(partition by report_date,login_appname,login_advname,viewer_appname,viewer_advname,status_b,flevel_b,fstatus_b,inblacklist) as cnt_b,
status_d,
flevel_d,
fstatus_d,
quality_d,
count(if(fstatus_d is null,null,waterid)) over(partition by report_date,login_appname,login_advname,viewer_appname,viewer_advname,status_d,flevel_d,fstatus_d,quality_d) as cnt_d,
status_i,
flevel_i,
fstatus_i,
quality_i,
count(if(fstatus_i is null,null,waterid)) over(partition by report_date,login_appname,login_advname,viewer_appname,viewer_advname,status_i,flevel_i,fstatus_i,quality_i) as cnt_i
  -- ,'${year_month}','${day_of_month}'
  from base
  where viewer_appname = '微米浏览器'
-- group by report_date,login_appname,login_advname,viewer_appname,viewer_advname,status_b,flevel_b,fstatus_b,inblacklist,status_d,flevel_d,fstatus_d,quality_d,status_i,flevel_i,fstatus_i,quality_i
order by status_b,fstatus_b,inblacklist,cnt_b,status_d,fstatus_d,quality_d,cnt_d,status_i,fstatus_i,quality_i,cnt_i
;







select id,reqtime,createtime,tagid,extagid from ods_wefix.t_ad_query_water_json where year_month = 201912 and day_of_month between 27 and 30 and tagid = '4y55uTCb33EGufc8yvEjSQ'
limit 50;


select distinct exchange_id,audit_app_id,audit_adver_id,audit_plan_id,apply_app_id,apply_plan_id,apply_adver_id,status,apply_user_id,audit_user_id from ods_wefix.exchange_info_tsv where apply_adver_id = '4y55uTCb33EGufc8yvEjSQ' or audit_adver_id = '4y55uTCb33EGufc8yvEjSQ';


select * from ods_wefix.t_ad_action_water_json where year_month = 201912 and day_of_month between 27 and 30 and sourceid = 'DZMhiEgUe8n79wv3F1G7XH' and extagid in ('4572EY23dBx8mzHpgqbhgD','86NobVk9Zy7twbUZJDFp7F')
limit 50;



select *
from ods_wefix.t_ad_query_water_json
where year_month = '201912' and day_of_month = '30'
and tagid in ('86NobVk9Zy7twbUZJDFp7F','2tMveHpPfG9bbpB4Q2gbRq')
and reqtime is null and createtime is null
-- and (test = 0 or test is null)
;

set hivevar:year_month=201912;

set hivevar:day_of_month=30;

with base as (
  select
  query_water.id                                                                            as id,
  substring(if(query_water.reqtime is null,query_water.createtime,query_water.reqtime),1,8) as report_date,
  action_water.status                                                                       as report_status,
  if(query_water.reqtime is null,'action','query')                                          as req_type,
  action_water.createtime                                                                   as action_ctime,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.apply_app_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.audit_app_id
  else null end                                                                             as plan_app_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.apply_user_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.audit_user_id
  else if(plan_info.user_id is null,null,plan_info.user_id) end                             as plan_user_id,
  if(query_water.acquisitionid is null,0,query_water.acquisitionid)                         as plan_id,
  query_water.extagid                                                                       as plan_adv_id,
  query_water.tagid                                                                         as adv_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.audit_app_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.apply_app_id
  else if(adv_info.app_id is null,null,adv_info.app_id) end                                 as adv_app_id,
  case
  when query_water.tagid = exchange_info.audit_adver_id then exchange_info.audit_user_id
  when query_water.tagid = exchange_info.apply_adver_id then exchange_info.apply_user_id
  else if(app_info.user_id is null,null,app_info.user_id) end                               as adv_user_id,
  if(adv_info.ad_type is null,0,adv_info.ad_type)                                           as ad_type,
  action_water.display                                                                      as action_display,
  action_water.isclick                                                                      as action_isclick
  from (
    select distinct id,reqtime,createtime,tagid,acquisitionid,extagid,year_month,day_of_month
    from ods_wefix.t_ad_query_water_json
    where year_month = '${year_month}' and day_of_month = '${day_of_month}' and (test = 0 or test is null)
    ) as query_water
  left join (
    select distinct waterid,createtime,status,display,isclick,year_month,day_of_month from ods_wefix.t_ad_action_water_json
    ) as action_water
  on query_water.id = action_water.waterid
  left join (
    select distinct exchange_id,
    audit_adver_id,audit_plan_id,audit_app_id,audit_user_id,
    apply_adver_id,apply_plan_id,apply_app_id,apply_user_id
    from ods_wefix.exchange_info_tsv
    where audit_app_id != 'NULL' and audit_user_id != 'NULL' and apply_app_id != 'NULL' and apply_user_id != 'NULL' and status > 6
    ) as exchange_info
  on (query_water.tagid = exchange_info.audit_adver_id and query_water.extagid = exchange_info.apply_adver_id and query_water.acquisitionid = exchange_info.apply_plan_id)
  or (query_water.tagid = exchange_info.apply_adver_id and query_water.extagid = exchange_info.audit_adver_id and query_water.acquisitionid = exchange_info.audit_plan_id)
  left join (
    select distinct acquisition_id,user_id from ods_wefix.acquisition_plan_tsv
    ) as plan_info on query_water.acquisitionid = plan_info.acquisition_id
  left join (
    select distinct advertise_id,app_id,ad_type from ods_wefix.advertisement_info_tsv
    ) as adv_info on query_water.tagid = adv_info.advertise_id
  left join (
    select distinct app_id,user_id from ods_wefix.app_info_tsv
    ) as app_info on adv_info.app_id = app_info.app_id
)
select distinct id,req_type,report_status,report_date,action_ctime,plan_adv_id,adv_id
from base
where report_date is null
and adv_id in ('86NobVk9Zy7twbUZJDFp7F','2tMveHpPfG9bbpB4Q2gbRq');




-- 奢分期 4y55uTCb33EGufc8yvEjSQ 爱租机 4572EY23dBx8mzHpgqbhgD
select waterid,createtime,sourceid,extagid,status,display,isclick from ods_wefix.t_ad_action_water_json where year_month = '201912' and day_of_month = '30' and sourceId = 'DZMhiEgUe8n79wv3F1G7XH' and extagid = '4572EY23dBx8mzHpgqbhgD';



select t1.waterid as waterid,createtime,status,display,if(isclick is null,0,isclick) as isclick,t1.year_month as year_month,t1.day_of_month as day_of_month
from (
  select distinct waterid,createtime,status,display,year_month,day_of_month from ods_wefix.t_ad_action_water_json
  where display = 1
  and year_month = '201912' and day_of_month = '30'
) as t1
left join (
  select distinct waterid,isclick,year_month,day_of_month from ods_wefix.t_ad_action_water_json
  where display = 0
) as t2 on t1.waterid = t2.waterid and t1.year_month = t2.year_month and t1.day_of_month = t2.day_of_month




select report_date,app_name,apply_cnt
from (
  select
  substring(create_time,1,8) as report_date,apply_app_id,count(distinct exchange_child_id) as apply_cnt
  from ods_wefix.exchange_info_child_tsv where status = 2
  and substring(create_time,1,6) = '202001' and substring(create_time,7,2) = '02'
  group by substring(create_time,1,8),apply_app_id
) as eict
left join (
  select distinct app_id,app_name from ods_wefix.app_info_tsv
) app on eict.apply_app_id = app.app_id
order by report_date,app_name
;



select report_date,app_name,change_cnt
from (
  select substring(createtime,1,8) as report_date,sourceid,count(distinct waterid) as change_cnt
  from ods_wefix.t_ad_action_water_json
  where year_month = '202001' and day_of_month = '02' and display = 1 and (status = 0 or status is null)
  group by substring(createtime,1,8),sourceid
) as action_water
left join (
  select distinct app_id,app_name from ods_wefix.app_info_tsv
) app on action_water.sourceid = app.app_id
order by report_date,app_name
;




set hivevar:year_month=201912;

set hivevar:day_of_month=18;

-- INSERT OVERWRITE TABLE dm_cf.data_preference PARTITION(year_month,day_of_month)
select
if(action_water.report_date is null,eict.report_date,action_water.report_date)          as report_date,
if(action_water.app_name_apply is null,eict.app_name_apply,action_water.app_name_apply) as app_name_apply,
if(action_water.app_name_audit is null,eict.app_name_audit,action_water.app_name_audit) as app_name_audit,
if(apply_cnt is null,0,apply_cnt)                                                       as apply_cnt,
if(change_cnt is null,0,change_cnt)                                                     as change_cnt
-- ,'${year_month}'  as  year_month,'${day_of_month}'  as  day_of_month
from (
  select
  report_date,
  if(app_apply.app_name is null,sourceid,app_apply.app_name) as app_name_apply,
  if(app_audit.app_name is null,ex_adv_id,app_audit.app_name) as app_name_audit,
  change_cnt
  from
  (
    select substring(createtime,1,8) as report_date,sourceid,ex_adv_id,count(distinct waterid) as change_cnt
    from (
      select distinct waterid,createtime,sourceid,extagid,year_month,day_of_month from ods_wefix.t_ad_action_water_json
      where display = 1 and (status = 0 or status is null)
      and year_month = '${year_month}' and day_of_month = '${day_of_month}'
      ) as action_water
    join (
      select id,year_month,day_of_month from ods_wefix.t_ad_query_water_json where (test = 0 or test is null)
      ) as query_water
    on action_water.year_month = query_water.year_month and action_water.day_of_month = query_water.day_of_month and action_water.waterid = query_water.id
    left join (
      select distinct advertise_id,app_id as ex_adv_id from ods_wefix.advertisement_info_tsv
      ) as adv_info on action_water.extagid = adv_info.advertise_id
    group by substring(createtime,1,8),sourceid,ex_adv_id
    ) as action_water
  left join (
    select distinct app_id,app_name from ods_wefix.app_info_tsv
    ) as app_apply on action_water.sourceid = app_apply.app_id
  left join (
    select distinct app_id,app_name from ods_wefix.app_info_tsv
    ) as app_audit on action_water.ex_adv_id = app_audit.app_id
) as action_water
full join (
  select
  report_date,
  if(app_apply.app_name is null,apply_app_id,app_apply.app_name) as app_name_apply,
  if(app_audit.app_name is null,apply_app_id,app_audit.app_name) as app_name_audit,
  apply_cnt
  from
  (
    select substring(create_time,1,8) as report_date,apply_app_id,audit_app_id,count(distinct exchange_child_id) as apply_cnt
    from ods_wefix.exchange_info_child_tsv
    where status = 2
    and substring(create_time,1,6) = '${year_month}' and substring(create_time,7,2) = '${day_of_month}'
    group by substring(create_time,1,8),apply_app_id,audit_app_id
    ) as eict
  left join (
    select distinct app_id,app_name from ods_wefix.app_info_tsv
    ) as app_apply on eict.apply_app_id = app_apply.app_id
  left join (
    select distinct app_id,app_name from ods_wefix.app_info_tsv
    ) as app_audit on eict.audit_app_id = app_audit.app_id
) as eict on action_water.report_date = eict.report_date and action_water.app_name_apply = eict.app_name_apply and action_water.app_name_audit = eict.app_name_audit
order by report_date,app_name_apply,app_name_audit
;





select distinct app_id,app_name from ods_wefix.app_info_tsv where app_name = '玩车头条查违章';

select substring(createtime,1,8) as report_date,sourceid,waterid
-- count(distinct waterid) as change_cnt
from ods_wefix.t_ad_action_water_json
where display = 1 and (status = 0 or status is null)
-- and year_month = '${year_month}' and day_of_month = '${day_of_month}'
and year_month = '202001' and day_of_month = '02'
and sourceid = '7gJJLVeWgtAiq3CrQ2r9Sj'
-- group by substring(createtime,1,8),sourceid
;


select * from ods_wefix.t_ad_action_water_json where waterid = '94290475';


select * from ods_wefix.t_ad_query_water_json where id = '94290475';


select * from dm_cf.data_preference;


INSERT OVERWRITE TABLE dm_cf.retention_overview
select
create_date,
login_date,
email,
mobile,
str_to_map(concat_ws(' | ',collect_set(concat(app_name,':',cast(status as string)))))  as  apps
,'202001'  as  year_month,'09'  as  day_of_month
from (
  select login_date,user_id,email,mobile,create_date
  from (
    select
    substring(login_time,1,8) as login_date,
    user_id,
    email,
    mobile,
    substring(create_time,1,8) as create_date,
    row_number() over(partition by user_id,substring(login_time,1,8) order by update_time desc) as od
    from ods_wefix.user_info_tsv
    where user_id not in ('87d787dc-ddaa-4dd9-b5f1-4e980177a376','2f25c429-1598-43e0-8f63-ba16ea3b1c73','dbfd22ac-9fe4-43f9-9e88-8ac0af92e52e')
    ) as tmp
  where od = 1
) as usr
left join (
  select distinct user_id,app_name,
  case status
  when 1    then '上线'
  when 2    then '下线'
  when 3    then '审核中'
  when 999  then '删除'
  else '未知' end as status
  from ods_wefix.app_info_tsv
) as app on usr.user_id = app.user_id
group by create_date,login_date,email,mobile
;




select * from ods_wefix.t_ad_query_water_json
where 1 = 1
and year_month = 202001
-- and day_of_month = 09
and tagid = 'JPs7fjjMPGFWWF9AGhxi36'
-- and extagid = ''
limit 50
;


set hivevar:year_month=202001;
set hivevar:day_of_month=08;


INSERT OVERWRITE TABLE dm_cf.addition_overview
select
if(create_date_usr is null,
  if(create_date_app is null,
    if(create_date_adv is null,
      create_date_pln,
      create_date_adv),
    create_date_app),
  create_date_usr)                as  create_date,
if(cnt_usr is null,0,cnt_usr)     as  cnt_usr,
if(cnt_app is null,0,cnt_app)     as  cnt_app,
if(cnt_adv is null,0,cnt_adv)     as  cnt_adv,
if(cnt_pln is null,0,cnt_pln)     as  cnt_pln
,'${year_month}'  as  year_month,'${day_of_month}'  as  day_of_month
from (
  select
  substring(create_time,1,8)      as  create_date_usr,
  count(distinct user_id)         as  cnt_usr
  from ods_wefix.user_info_tsv
  group by substring(create_time,1,8)
) as usr
full join (
  select
  substring(create_time,1,8)      as  create_date_app,
  count(distinct app_id)          as  cnt_app
  from ods_wefix.app_info_tsv
  group by substring(create_time,1,8)
) as app on create_date_usr = create_date_app
full join (
  select
  substring(create_time,1,8)      as  create_date_adv,
  count(distinct advertise_id)    as  cnt_adv
  from ods_wefix.advertisement_info_tsv
  group by substring(create_time,1,8)
) as adv on create_date_usr = create_date_adv or create_date_app = create_date_adv
full join (
  select
  substring(create_time,1,8)      as  create_date_pln,
  count(distinct acquisition_id)  as  cnt_pln
  from ods_wefix.acquisition_plan_tsv
  group by substring(create_time,1,8)
) as pln on create_date_usr = create_date_pln or create_date_app = create_date_pln or create_date_adv = create_date_pln
order by create_date
;




select * from dm_cf.addition_overview;
select * from dm_cf.adt_admin;
select * from dm_cf.adt_admin;
select * from dm_cf.advertising_space;
select * from dm_cf.data_preference;
select * from dm_cf.retention_overview;






















