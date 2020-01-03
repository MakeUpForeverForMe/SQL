sqoop eval --connect jdbc:oracle:thin:@10.83.16.3:1521:orcl --username ETLREADER --password etlreader2019 -e 'SELECT DBMS_METADATA.GET_DDL("TABLE","CUSTOMERINFO") FROM DUAL'

sqoop eval --connect jdbc:oracle:thin:@10.1.16.2:1521:ORCL --username ETLREADER --password etlreader2019 -e "select * from user_tab_columns where table_name = 'T_LOAN_CONTRACT_INFO'"

sqoop eval --connect jdbc:oracle:thin:@10.1.16.2:1521:ORCL --username ETLREADER --password etlreader2019 -e "select * from all_tab_columns where table_name = 'T_LOAN_CONTRACT_INFO'"

sqoop eval --connect jdbc:oracle:thin:@10.1.16.2:1521:ORCL --username ETLREADER --password etlreader2019 -e "select table_name from user_tables"

sqoop list-databases --connect jdbc:oracle:thin:@10.1.16.2:1521:ORCL --username ETLREADER --password etlreader2019

sqoop eval --connect jdbc:mysql://10.80.16.7:3306/starsource --username BDUser_R --password xy@Eh93AmnCkMbiU -e 'select * from ORG_INFO'


# MySQL
# 将关系型数据的表结构复制到hive中
for table in ADS_INBOUND ADS_RETURN CLIENT_INFO EVENT_LOGGER ORG_INFO PRODUCT_INFO RECOMMEND_RECON SOURCE_ORG_INFO STRATEGY_MATCHING; do
  sqoop create-hive-table --connect jdbc:mysql://10.80.16.7:3306/starsource --table $table --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-database ods_source_old --hive-table ${table}_tmp
done


sqoop create-hive-table --connect jdbc:mysql://10.80.16.7:3306/starsource --table ORG_INFO --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-database ods_source_old --hive-table org_info



# 查看Hive中的表结构
for table in ADS_INBOUND ADS_RETURN CLIENT_INFO EVENT_LOGGER ORG_INFO PRODUCT_INFO RECOMMEND_RECON SOURCE_ORG_INFO STRATEGY_MATCHING; do
  beeline -n hdfs -u jdbc:hive2://10.80.176.20:10000 --hivevar table=$table -e 'show create table ods_source_old.${table};'
done


# 从关系数据库导入文件到hive中 --direct --fields-terminated-by '\t'
for table in CLIENT_INFO ORG_INFO PRODUCT_INFO SOURCE_ORG_INFO; do
  sqoop import --connect jdbc:mysql://10.80.16.7:3306/starsource --table $table --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-import --hive-database ods_source_old --hive-table $table
done





sqoop import \
-m 1 \
--connect jdbc:mysql://10.80.16.7:3306/starsource --table ORG_INFO \
--username BDUser_R --password xy@Eh93AmnCkMbiU \
--delete-target-dir \
--hive-import \
--hive-database ods_source_old \
--hive-table org_info

--as-parquetfile -z --compression-codec org.apache.hadoop.io.compress.SnappyCodec \



for table in ADS_INBOUND ADS_RETURN EVENT_LOGGER RECOMMEND_RECON STRATEGY_MATCHING; do
  sqoop import --connect jdbc:mysql://10.80.16.7:3306/starsource --table STRATEGY_MATCHING --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-import --hive-database ods_source_old --hive-table strategy_matchin --hive-partition-key  --hive-partition-value --hive-overwrite
done



# Oracle
sqoop import \
-m 1 \
--connect jdbc:oracle:thin:@10.1.16.2:1521:orcl --table $table \
--username ETLREADER --password etlreader2019 \
--hive-database ods_link --hive-table $table









# 测试
# MySQL
for table in ADS_INBOUND ADS_RETURN CLIENT_INFO EVENT_LOGGER ORG_INFO PRODUCT_INFO RECOMMEND_RECON SOURCE_ORG_INFO STRATEGY_MATCHING; do
  sqoop create-hive-table --connect jdbc:mysql://10.80.16.7:3306/starsource --table $table --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-database ods_source_old --hive-table ${table}_tmp
done

for table in CLIENT_INFO ORG_INFO PRODUCT_INFO SOURCE_ORG_INFO; do
  sqoop import --connect jdbc:mysql://10.80.16.7:3306/starsource --table $table --username BDUser_R --password xy@Eh93AmnCkMbiU --hive-import --hive-database ods_source_old --hive-table $table
done




# Oracle
# 建表
for table in T_BORROWER_INFO T_ASSOCIATES_INFO T_GUARANTY_CAR_INFO T_LOAN_CONTRACT_INFO; do
  sqoop create-hive-table --connect jdbc:oracle:thin:@10.83.16.3:1521:orcl --table $table --username ABSBANK --password absbank --hive-database ods_link --hive-table $table
done

for table in CUSTOMERINFO HISASSETBASICINFO HISASSETBASICINFONEW; do
  sqoop create-hive-table --connect jdbc:oracle:thin:@10.83.16.3:1521:orcl --table $table --username ABSBANK --password absbank --hive-database ods_bank --hive-table $table
done

# 导数据
for table in T_BORROWER_INFO T_ASSOCIATES_INFO T_GUARANTY_CAR_INFO T_LOAN_CONTRACT_INFO; do
  sqoop import \
  -m 1 \
  --connect jdbc:oracle:thin:@10.83.16.3:1521:orcl --table $table \
  --username ABSBANK --password absbank \
  --hive-database ods_link --hive-table $table
done

for table in CUSTOMERINFO HISASSETBASICINFO HISASSETBASICINFONEW; do
  sqoop import \
  -m 1 \
  --connect jdbc:oracle:thin:@10.83.16.3:1521:orcl --table $table \
  --username ABSBANK --password absbank \
  --hive-database ods_bank --hive-table $table
done




