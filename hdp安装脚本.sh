#!/bin/bash -e
beeline -u jdbc:hive2://10.80.176.20:10000 -n hdfs

*uy8J!Dp5UxTOPo

8Gpn2N8b0SXwahJ

# ---------------===============>>>>>>>>>>>>>>>     有需要时配置     <<<<<<<<<<<<<<<===============--------------- #
# vim命令失效时根据缺少的文件进行安装 vim-common.x86_64、vim-enhanced.x86_64、vim-filesystem.x86_64、vim-minimal.x86_64
yum list installed | grep vim
yum install -y vim

# 安装代码自动补全
yum install bash-completion

# 需要telnet时安装或卸载
yum list telnet* | grep -v server | grep -wi telnet | xargs yum install -y

yum list telnet* | grep -iA 1 install | grep telnet | xargs yum remove -y


# ---------------===============>>>>>>>>>>>>>>>     主机需要配置     <<<<<<<<<<<<<<<===============--------------- #
yum install -y httpd
yum install -y libtirpc
yum install -y yum-utils createrepo yum-plugin-priorities

yum install -y telnet
yum install -y ftp
yum install -y lrzsz

ftp -n << eof
open 10.83.0.32
user it-dev 058417gv
binary
cd /ximing.wei
prompt off
mget *
close
bye
eof


# ---------------===============>>>>>>>>>>>>>>>     所有都需要配置     <<<<<<<<<<<<<<<===============--------------- #
# 向服务器中添加命令
echo -e "\n\nalias ll='ls -lhi --color=auto'\nalias la='ll -A'\n" >> /etc/profile && tail /etc/profile

# 生产
# ips[0]='10.80.176.3:master' ips[1]='10.80.176.35:masterslave' ips[2]='10.80.176.2:slave1' ips[3]='10.80.176.26:slave2' ips[4]='10.80.176.48:slave3' ips[5]='10.80.176.20:spark' ips[6]='10.80.176.13:etl' ips[7]='10.80.176.21:ambari'

# 测试
ips[0]='10.83.96.4:master' ips[1]='10.83.96.6:masterslave' ips[2]='10.83.96.8:slave1' ips[3]='10.83.96.12:slave2' ips[4]='10.83.96.15:slave3' ips[5]='10.83.96.13:etl' ips[6]='10.83.96.9:spark'

# 增加主机识别
echo -e '\n\n' >> /etc/hosts; for ip in ${ips[@]}; do echo ${ip//:/ } >> /etc/hosts; done; tail /etc/hosts

# 修改主机名
grep -Po "$(ifconfig | grep -A 1 eth0 | grep inet | awk '{print $2}')[ ]+\K[^ ]+" /etc/hosts > /etc/hostname && tail /etc/hostname

# 永久关闭防火墙
chkconfig iptables off
# 关闭selinux
sed -d 's/SELINUX=.*/SELINUX=disabled/' /etc/sysconfig/selinux
# 安装ambari需要配置的
sed -i 's/verify=.*/verify=disable/g' /etc/python/cert-verification.cfg && cat /etc/python/cert-verification.cfg

# 增加免密登陆并验证
ipname_ip(){ ipname=(${1//:/ }); ip=${ipname[0]}; name=${ipname[1]}; }; [[ ! -f ~/.ssh/id_rsa.pub || ! -f ~/.ssh/id_rsa ]] && ssh-keygen -t rsa; for ipname in ${ips[@]}; do ipname_ip $ipname; ssh-copy-id -i $ip; ssh $name 'hostname'; ssh $ip 'hostname'; done


# 检测是否有安装过jdk、scala、mysql、mongodb
yum list installed | grep -vi scalar | grep -Ei 'scala|jdk|mysql|mongodb'
yum list installed | grep -vi scalar | grep -Ei 'scala|jdk|mysql|mongodb' | xargs yum remove -y


cat /proc/sys/vm/swappiness

sysctl -w vm.swappiness=10;echo vm.swappiness = 10 >> /etc/sysctl.conf && cat /proc/sys/vm/swappiness && cat /etc/sysctl.conf

# ---------------===============>>>>>>>>>>>>>>>     主机需要配置     <<<<<<<<<<<<<<<===============--------------- #
# 安装Java和Scala，添加JAVA_HOME
for file in /root/*; do
  [[ ! $file =~ jdk && ! $file =~ scala ]] && continue
  echo -e "\n# ----------  $file  ---------- #\n"
  for ipname in ${ips[@]}; do
    ipname_ip $ipname
    echo -e "\n# --------------->>>>>>>>>>  ${ipname[*]}  <<<<<<<<<<--------------- #\n"
    echo -e "\n# ----------  scp start  ---------- #\n"
    scp $file $ip:/root/
    echo -e "\n# ----------  scp end  ---------- #\n"
    echo -e "\n# ----------  rpm install start  &&  rm $file  &&  test $file  ---------- #\n"
    ssh $name "
    rpm -ivh $file
    rm -f $file
    java -version; scala -version
    [[ -z '$(grep -EA 4 -B 2  '^JAVA_HOME' /etc/profile)' ]] && echo -e '\n\nJAVA_HOME=/usr/java/default\nPATH=\$PATH:\$JAVA_HOME/bin\n\nexport PATH JAVA_HOME\n' >> /etc/profile && tail -n 20 /etc/profile || echo -e '\n已添加nJAVA_HOME\n'
    "
    echo -e "\n# ----------  rpm install end  &&  rm $file  &&  test $file  ---------- #\n"
  done
done

mkdir -p /usr/share/java/

cp /root/mysql-connector-java.jar /usr/share/java/

# 硬盘挂载配置
# 查看是否有挂载硬盘
fdisk -l

# 初始化磁盘 未具体写明的直接回车
fdisk /dev/vdb

n 回车
四次回车
w 回车

# 查看是否有挂载硬盘
fdisk -l

# 格式化
mkfs -t ext4 /dev/vdb1

# 挂载的目录为hadoop
mkdir /hadoop
mount /dev/vdb1 /hadoop

vim /etc/fstab
echo '/dev/vdb1            /hadoop              ext4       defaults              0 1' >> /etc/fstab


# 重启服务器
reboot


# 开启httpd
httpd -k start
# 创建hdp的文件夹
mkdir /var/www/html/hdp
# 解压hdp的文件到httpd的hdp文件夹下
for file in /root/*; do
  if [[ $file =~ ambari ]]; then
    tar -zxvf $file -C /var/www/html
  elif [[ $file =~ HDP ]]; then
    tar -zxvf $file -C /var/www/html/hdp
  fi
done

# vim repo 并把gpgcheck修改为0
vim /var/www/html/hdp/ambari/centos7/2.7.3.0-139/ambari.repo
# http://10.80.176.21/hdp/ambari/centos7/2.7.3.0-139
http://10.83.96.9/hdp/ambari/centos7/2.7.3.0-139
vim /var/www/html/hdp/HDP/centos7/3.1.0.0-78/hdp.repo
# http://10.80.176.21/hdp/HDP/centos7/3.1.0.0-78
# http://10.80.176.21/hdp/HDP-UTILS/centos7/1.1.0.22
http://10.83.96.9/hdp/HDP/centos7/3.1.0.0-78
http://10.83.96.9/hdp/HDP-UTILS/centos7/1.1.0.22

# 创建repo源
createrepo  /var/www/html/

# 使每台服务器都有repo文件
for name in $(grep -E '10.83.96.*' /etc/hosts | awk '{print $2}'); do
  echo -e "\n-----------------$name-----------------\n"
  echo -e "\n----------------- scp 开始 -----------------\n"
  scp /var/www/html/hdp/ambari/centos7/2.7.3.0-139/ambari.repo $name:/etc/yum.repos.d
  scp /var/www/html/hdp/HDP/centos7/3.1.0.0-78/hdp.repo $name:/etc/yum.repos.d
  echo -e "\n----------------- scp 结束 -----------------\n"
  echo -e "\n----------------- yum clean makecache 开始 -----------------\n"
  yum clean all
  yum makecache
  echo -e "\n----------------- yum clean makecache 结束 -----------------\n"
done

yum -y install ambari-server

# 安装mysql客户端
tar -zxvf /root/mysql-5.5.62-linux-glibc2.12-x86_64.tar.gz
mv /root/mysql-5.5.62-linux-glibc2.12-x86_64 /root/mysql-5.5.62

# 在MySQL中创建ambari库和hive库
# 生产环境
# /root/mysql-5.5.62/bin/mysql -h10.80.176.22 -umeta ambari -pmeta2015 < /var/lib/ambari-server/resources/Ambari-DDL-MySQL-CREATE.sql
# 测试环境
/root/mysql-5.5.62/bin/mysql -h10.83.96.7 -uroot ambari -pRRDdjhPULOdZ703 < /var/lib/ambari-server/resources/Ambari-DDL-MySQL-CREATE.sql

ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar

ambari-server setup

# ambari的用户设置为root，但默认用户还是admin
y
回车
2 Custom JDK
Path to JAVA_HOME:/usr/java/default
GPL LZO : 回车
y
# 以下是关于mysql的配置 未写的即为默认即可
3 MySQL / MariaDB
Hostname(mysql的地址): 10.83.96.7
Username (ambari): root
RRDdjhPULOdZ703

# 生产------启动后登录http://10.80.176.21:8080查看
# 测试------启动后登录http://10.83.96.9:8080查看
ambari-server start


# ******* 超重要 这一步选择 Use Local Repositoty ***** #
# 生产
# http://10.80.176.21/hdp/HDP/centos7/3.1.0.0-78
# http://10.80.176.21/hdp/HDP-UTILS/centos7/1.1.0.22
# 测试
http://10.83.96.9/hdp/HDP/centos7/3.1.0.0-78
http://10.83.96.9/hdp/HDP-UTILS/centos7/1.1.0.22

# 生产
ambari
spark
master
slavemaster
slave1
slave2
slave3
etl

# 测试
spark
master
masterslave
slave1
slave2
slave3
etl

# 生产
# jdbc:mysql://10.80.176.22:3306/hive?createDatabaseIfNotExist=true&useSSL=false&characterEncoding=UTF-8
# 测试
jdbc:mysql://10.83.96.7:3306/hive?createDatabaseIfNotExist=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8
RRDdjhPULOdZ703


# 修改用户密码
passwd hdfs



# 开启namenode
# 在standby的服务器上执行
su -l hdfs -c "/usr/hdp/current/hadoop-hdfs-namenode/../hadoop/sbin/hadoop-daemon.sh start namenode"
# 格式化
su -l hdfs -c "hdfs namenode -bootstrapStandby -force"



# 邮件：ambari服务告警
# ambari服务器
# 修改文件
/var/lib/ambari-server/resources/alert-templates-custom.xml
# <![CDATA[数据中心Ambari监控报告：OK[$summary.getOkCount()], Warning[$summary.getWarningCount()], Critical[$summary.getCriticalCount()], Unknown[$summary.getUnknownCount()]]]>
<h3 class="panel-title">测试 : 有错误产生</h3>
# 修改文件，最后一行添加一下内容
/etc/ambari-server/conf/ambari.properties
alerts.template.file=/var/lib/ambari-server/resources/alert-templates-custom.xml
# 邮箱配置
10.80.0.133  apb-report@service.weshareholdings.com  &kQ4TOWerGlfpUm7  ximing.wei@weshareholdings.com





# --------------------     相关配置     --------------------#
# hdfs相关配置 hadoop-policy
# 管理员授权协议
# security.admin.operations.protocol.acl=hdfs
# 刷新授权策略协议
# security.refresh.policy.protocol.acl=hdfs
# 刷新用户映射协议
# security.refresh.usertogroups.mappings.protocol.acl=hdfs
# hadoop代理用户管理组
# hadoop.proxyuser.admin.groups=*

# dfs权限已启用 -- hdfs-site.xml  # 默认 true 可不修改
# dfs.permissions.enabled=false




# hive相关配置   hive-site.xml
# 设置动态分区
# hive.exec.dynamic.partition=true # 默认
# hive.exec.dynamic.partition.mode=nonstrict # 默认


# 设置reduce执行数
# 设置每个reduce任务处理的数据量(默认1024*1024*64=67108864=64M) # 默认
# hive.exec.reducers.bytes.per.reducer=67108864
# 设置reduce个数(默认1009) # 默认
# hive.exec.reducers.max=1009
# 设置reduce task的任务数--添加
mapred.reduce.tasks=50
# group的键对应的记录条数超过这个值则会进行分拆,值根据具体数据量设置(默认未配置)--添加
hive.groupby.mapaggr.checkinterval=100000


# 设置多进程
# 设置多线程并行度
hive.exec.parallel=true # 勾选
hive.exec.parallel.thread.number=16 # 默认8


# hive设置udf目录 hive-site.xml--添加
hive.reloadable.aux.jars.path=/hadoop/hive-udf-aux


# 权限设置
# 开启用户身份认证
hive.security.authorization.enabled=true # 默认true
# 阻止没有权限的用户进行表删除操作
hive.metastore.authorization.storage.checks=true # 默认false
# 修改用户代理
hive.server2.enable.doAs=true # 默认false
# 添加
# 对创建者赋予所有权限
# hive.security.authorization.createtable.group.grants=all
# hive.security.authorization.createtable.role.grants=all
# hive.security.authorization.createtable.user.grants=all
hive.security.authorization.createtable.owner.grants=all
# 对用户的权限使用的权限管理类
# hive.security.authorization.task.factory=org.apache.hadoop.hive.ql.parse.authorization.HiveAuthorizationTaskFactoryImpl
# Hive的管理员用户角色--添加
hive.users.in.admin.role=hdfs;spark
# Hive的新建文件的默认权限(0002为掩码)
# hive.files.umask.value=0002
# 设置同时连接的用户数
hive.server2.limit.connections.per.user=50
# 设置同时连接的ip数
hive.server2.limit.connections.per.ipaddress=50
# 设置同时连接的每个用户的ip数
hive.server2.limit.connections.per.user.ipaddress=50


# hive加载数据所有者 -- hive-site -- hive-interactive-site
hive.load.data.owner=spark



# 关闭Hive的ACID功能  -- 可不关闭
# hive严格管理表 -- hive-interactive-site 默认true -- hive-site 默认true
# hive.strict.managed.tables=false
# hive仅作为插入创建 -- hive-site 默认true(勾选)
# hive.create.as.insert.only=false
# Metastore采用ACID方式创建 -- hive-site 默认true(勾选)
# metastore.create.as.acid=false




# hdfs-site --(默认022)
fs.permissions.umask-mode=022
# hdfs-site --(默认hdfs)
dfs.cluster.administrators=hdfs



# 客户端超时设置
# Advanced tez-interactive-site
tez.session.am.dag.submit.timeout.secs=0 # 修改
# Tez设置
# Advanced tez-site
tez.session.am.dag.submit.timeout.secs=0 # 修改
tez.session.client.timeout.secs=0 # 修改


# 开启sparkthrift
# spark2-hive-site-override
metastore.catalog.default=hive # 修改
# hive-site.xml(默认true) # 勾选
hive.strict.managed.tables=false # 勾选
# spark2-hive-site-override -- 开启用户代理
# hive.server2.enable.doAs=false



# --------------------     问题解决     --------------------#
# 问题1： There are 17 stale alerts from 6 host(s):
# 重启相应的ambari-agent
ambari-agent restart

# 问题2： heartbeat lost
ambari-agent restart







# --------------------     卸载Ambari     --------------------#
ambari-server stop
ambari-agent stop



for hdp in $(yum list installed | grep @HDP) $(yum list installed | grep ambari) $(yum list installed | grep atlas); do
  echo $hdp
  yum erase ambari-agent -y
  ambari-server reset -y;yum erase ambari-server -y
  yum remove -y $hdp
done






userdel -r activity_analyzer
userdel -r ambari-qa
userdel -r ams
userdel -r atlas
userdel -r falcon
userdel -r flume
userdel -r hbase
userdel -r hcat
userdel -r hdfs
userdel -r hive
userdel -r kafka
userdel -r knox
userdel -r livy
userdel -r mapred
userdel -r oozie
userdel -r spark
userdel -r sqoop
userdel -r storm
userdel -r tez
userdel -r yarn
userdel -r yarn-ats
userdel -r zeppelin
userdel -r zookeeper
rm -rf /etc/ambari-*
rm -rf /etc/falcon
rm -rf /etc/flume
rm -rf /etc/hadoop
rm -rf /etc/hbase
rm -rf /etc/hive
rm -rf /etc/hive-hcatalog
rm -rf /etc/hive-webhcat
rm -rf /etc/hive2
rm -rf /etc/kafka
rm -rf /etc/knox
rm -rf /etc/oozie
rm -rf /etc/phoenix
rm -rf /etc/pig
rm -rf /etc/slider
rm -rf /etc/spark
rm -rf /etc/spark2
rm -rf /etc/sqoop
rm -rf /etc/storm
rm -rf /etc/storm-slider-client
rm -rf /etc/tez
rm -rf /etc/tez_hive2
rm -rf /etc/zookeeper
rm -rf /hadoop/*
rm -rf /home/*
rm -rf /opt/hadoop
rm -rf /tmp/ambari-qa
rm -rf /tmp/hadoop
rm -rf /tmp/hadoop-hdfs
rm -rf /tmp/hive
rm -rf /usr/bin/accumulo
rm -rf /usr/bin/atlas-start
rm -rf /usr/bin/atlas-stop
rm -rf /usr/bin/beeline
rm -rf /usr/bin/falcon
rm -rf /usr/bin/flume-ng
rm -rf /usr/bin/hadoop
rm -rf /usr/bin/hbase
rm -rf /usr/bin/hcat
rm -rf /usr/bin/hdfs
rm -rf /usr/bin/hive
rm -rf /usr/bin/hiveserver2
rm -rf /usr/bin/kafka
rm -rf /usr/bin/mahout
rm -rf /usr/bin/mapred
rm -rf /usr/bin/oozie
rm -rf /usr/bin/oozied.sh
rm -rf /usr/bin/phoenix-psql
rm -rf /usr/bin/phoenix-queryserver
rm -rf /usr/bin/phoenix-sqlline
rm -rf /usr/bin/phoenix-sqlline-thin
rm -rf /usr/bin/pig
rm -rf /usr/bin/ranger-admin-start
rm -rf /usr/bin/ranger-admin-stop
rm -rf /usr/bin/ranger-kms
rm -rf /usr/bin/ranger-usersync-start
rm -rf /usr/bin/ranger-usersync-stop
rm -rf /usr/bin/slider
rm -rf /usr/bin/sqoop
rm -rf /usr/bin/sqoop-codegen
rm -rf /usr/bin/sqoop-create-hive-table
rm -rf /usr/bin/sqoop-eval
rm -rf /usr/bin/sqoop-export
rm -rf /usr/bin/sqoop-help
rm -rf /usr/bin/sqoop-import
rm -rf /usr/bin/sqoop-import-all-tables
rm -rf /usr/bin/sqoop-job
rm -rf /usr/bin/sqoop-list-databases
rm -rf /usr/bin/sqoop-list-tables
rm -rf /usr/bin/sqoop-merge
rm -rf /usr/bin/sqoop-metastore
rm -rf /usr/bin/sqoop-version
rm -rf /usr/bin/storm
rm -rf /usr/bin/storm
rm -rf /usr/bin/storm-slider
rm -rf /usr/bin/storm-slider
rm -rf /usr/bin/worker-lanucher
rm -rf /usr/bin/worker-lanucher
rm -rf /usr/bin/yarn
rm -rf /usr/bin/zookeeper-client
rm -rf /usr/bin/zookeeper-server
rm -rf /usr/bin/zookeeper-server-cleanup
rm -rf /usr/hadoop
rm -rf /usr/hdp
rm -rf /usr/lib/ambari-*
rm -rf /usr/lib/flume
rm -rf /usr/lib/python2.6/site-packages/ambari_*
rm -rf /usr/lib/python2.6/site-packages/resource_management
rm -rf /usr/lib/storm
rm -rf /var/hadoop
rm -rf /var/hadoop
rm -rf /var/lib/ambari*
rm -rf /var/lib/flume
rm -rf /var/lib/hadoop-hdfs
rm -rf /var/lib/hadoop-mapreduce
rm -rf /var/lib/hadoop-yarn
rm -rf /var/lib/hive
rm -rf /var/lib/knox
rm -rf /var/lib/oozie
rm -rf /var/lib/slider
rm -rf /var/lib/spark2
rm -rf /var/lib/zookeeper
rm -rf /var/log/ambari-agent
rm -rf /var/log/ambari-metrics-monitor
rm -rf /var/log/ambari-server
rm -rf /var/log/falcon
rm -rf /var/log/flume
rm -rf /var/log/hadoop
rm -rf /var/log/hadoop-hdfs
rm -rf /var/log/hadoop-mapreduce
rm -rf /var/log/hadoop-yarn
rm -rf /var/log/hbase
rm -rf /var/log/hive
rm -rf /var/log/hive-hcatalog
rm -rf /var/log/knox
rm -rf /var/log/oozie
rm -rf /var/log/spark
rm -rf /var/log/sqoop
rm -rf /var/log/storm
rm -rf /var/log/webhcat
rm -rf /var/log/zookeeper
rm -rf /var/run/ambari-metrics-collector
rm -rf /var/run/ambari-metrics-monitor
rm -rf /var/run/falcon
rm -rf /var/run/flume
rm -rf /var/run/hadoop
rm -rf /var/run/hadoop-hdfs
rm -rf /var/run/hadoop-mapreduce
rm -rf /var/run/hadoop-yarn
rm -rf /var/run/hbase
rm -rf /var/run/hive
rm -rf /var/run/hive-hcatalog
rm -rf /var/run/kafka
rm -rf /var/run/oozie
rm -rf /var/run/spark
rm -rf /var/run/sqoop
rm -rf /var/run/storm
rm -rf /var/run/webhcat
rm -rf /var/run/zookeeper
rm -rf /var/tmp/oozie
rm -rf /tmp/*


yum clean all
