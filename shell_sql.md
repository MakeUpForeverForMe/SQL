[TOC]
# 1、服务器信息
## 1.1 新集群
### 1.1.1 生产
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

### 1.1.2 测试
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

## 1.2 旧集群
### 1.2.1 生产
|    hostname   |  ip或网址  | 用户 |     密码    | 备注 |
|---------------|------------|------|-------------|------|
| BSPRD-Hadoop1 | 10.80.0.20 | root | Xfx2018@)!* |      |
| BSPRD-Hadoop2 | 10.80.0.23 | root | Xfx2018@)!* |      |
| BSPRD-Hadoop3 | 10.80.0.29 | root | Xfx2018@)!* |      |
| 数据库mysql   | 10.80.16.3 | root | Xfx2018@)!* |      |

### 1.2.2 测试
|   hostname  |  ip或网址   | 用户 |       密码       | 备注 |
|-------------|-------------|------|------------------|------|
| bssit-cdh-1 | 10.83.80.5  | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-2 | 10.83.80.7  | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-3 | 10.83.80.14 | root | !W$WdwY7U%pe)YkQ |      |
| bssit-cdh-4 | 10.83.80.2  | root | !W$WdwY7U%pe)YkQ |      |
| 数据库mysql | 10.83.96.10 | root | !W$WdwY7U%pe)YkQ |      |







# 2、记录各项操作
## 2.1 Shell 命令
### 2.1.1 基础 Shell 命令
```shell
#!/bin/bash -e

. /etc/profile
. ~/.bash_profile
base_dir=$(dirname "${BASH_SOURCE[0]}")

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

# 查看磁盘大小及使用率
df -h

# 查看当前目录下每个文件夹的大小
du -sh *

# 添加基础操作命令
# /etc/profile
export TIME_STYLE='+%F %T'      # 设置系统默认时间格式为：yyyy-MM-dd HH:mm:ss
alias ll='ls -lh --color=auto'  # 修改 ll 命令带有文件大小
alias la='ll -A'                # 设置 la 命令可以查看到隐藏文件
```

### 2.1.2 Shell 命令的练习
```shell
# 匹配到 key ，取 value ----- result : gjdogjd$h*^^54
echo '{"biType": gjdogjd$h*^^54,"gdsdfgd":45146}' | grep -Po 'biType[": ]+\K[^" ,]+'

# 将具有一定格式的文件，按格式切分成多份
base_dir=$(dirname "${BASH_SOURCE[0]}")

oldIFS=$IFS
IFS=%

lists=($(cat $base_dir/saas.hql | grep -B1 -A5 -i 'drop' | sed 's/--$/#/g'))

for list in ${lists[@]}; do
  file_name=$(echo $list | grep -Poi 'drop.*\.\K[^;]+' | sed 's/`//g').hql
  echo $list > $base_dir/hql/$file_name
done

```

### 2.1.3 Shell 中 $ 的用法
```shell
# 各项$的用处
$0 程序的名称
$n 程序的第n个参数
$# 程序的参数个数
$$ 当前脚本进程ID
$! 返回最后一个后台运行程序的进程ID
$? 显示最后命令的退出状态。0表示没有错误，其他任何值表明有错误
$- 显示shell使用的当前选项，与set命令功能相同
$* 显示所有参数，以字符串的方式返回
$@ 显示所有参数，以数组的方式返回
```

### 2.1.4 Shell 中判断选项的用法
```shell
# 判断时各选项的用法
-e  判断对象是否存在
-d  判断对象是否存在，并且为目录
-f  判断对象是否存在，并且为常规文件
-L  判断对象是否存在，并且为符号链接
-h  判断对象是否存在，并且为软链接
-s  判断对象是否存在，并且长度不为0
-r  判断对象是否存在，并且可读
-w  判断对象是否存在，并且可写
-x  判断对象是否存在，并且可执行
-O  判断对象是否存在，并且属于当前用户
-G  判断对象是否存在，并且属于当前用户组
-n  判断变量是否非空
-z  判断变量是否为空

-nt 判断file1是否比file2新
[[ "/data/file1" -nt "/data/file2" ]] && echo true || echo false
-ot 判断file1是否比file2旧
[[ "/data/file1" -ot "/data/file2" ]] && echo true || echo false
 
-eq 等于
-ne 不等于
-gt 大于
-lt 小于
-ge 大于等于
-le 小于等于
```

### 2.1.5 Shell 中渲染样式的设置
```shell
echo -e '\033[0m关闭所有属性\033[0m'
echo -e '\033[1m设置高亮\033[0m'
echo -e '\033[4m下划线\033[0m'
echo -e '\033[5m闪烁\033[0m'
echo -e '\033[7m反显\033[0m'
echo -e '\033[8m消隐\033[0m'
echo -e '\033[30m -- \33[37m设置前景色黑,红,绿,棕,蓝,紫,青,白\033[0m'
echo -e '\033[40m -- \33[47m设置背景色黑,红,绿,棕,蓝,紫,青,白\033[0m'
echo -e '\033[nA光标上移n行\033[0m'
echo -e '\033[nB光标下移n行\033[0m'
echo -e '\033[nC光标右移n行\033[0m'
echo -e '\033[nD光标左移n行\033[0m'
echo -e '\033[y;xH设置光标位置\033[0m'
echo -e '\033[2J清屏\033[0m'
echo -e '\033[K清除从光标到行尾的内容\033[0m'
echo -e '\033[s保存光标位置\033[0m'
echo -e '\033[u恢复光标位置\033[0m'
echo -e '\033[?25l隐藏光标\033[0m'
echo -e '\033[?25h显示光标\033[0m'

# 不换行动态显示当前时间
for i in `seq 10`; do
  echo -n -e "\r\033[K\033[0m"
  echo -n -e "current time : \033[34m"
  echo -n `date +'%F %T'`
  sleep 1
done; echo -e "\033[0m"
```

### 2.1.6 Shell 中 shift 的用法
```shell
shift 左移1个参数
shift 5 左移5个参数
```

### 2.1.7 Shell 中 grep 的用法
```shell
grep -wq file_name # 完全匹配字符所在行并不输出任何信息，只输出匹配结果
```

### 2.1.8 Shell 中 tr 的用法（translating）
```shell
# 匹配替换
echo tableName | tr '[A-Z]' '[a-z]' # 将小写变为大写 TABLENAME
echo TABLENAME | tr '[A-Z]' '[a-z]' # 将大写变为小写 tableName
```

### 2.1.9 Shell 中 解压缩命令
```shell
# 压缩
tar -zcvf aa.tar.gz aa  # tar-gzip
tar -jcvf aa.tar.bz2 aa # tar-bzip2
gzip -9 aa              # gzip (9为压缩比例，可为1-9。压缩后文件删除)
bzip2 aa                # bzip2
# 解压
tar -zxvf aa.tar.gz     # tar-gzip
tar -jxvf aa.tar.bz2    # tar-bzip2
gzip -d aa.gz           # gzip
bzip2 -d aa.bz2         # bzip2
# 查看压缩文件
zcat aa.gz              # gzip
bzcat aa.bz2            # bzip2
```

### 2.1.10 Shell 中 yum 命令
```shell
# 安装dos2unix
yum install -y dos2unix

# 更新 yum 源
yum clean all && yum makecache
yum update kernel  -y
# reboot # 重启

# 获取服务器的版本号
uname -a # Linux node47 3.10.0-1062.4.1.el7.x86_64 ...
```

### 2.1.11 Shell 中 git 命令
```shell
# git的提交代码(-u update)
git init # 第一次需要
git add [-u] file
git commit -m '注释'
git commit --amend # 注释填写错误时修改
git remote add origin git@github.com:MakeUpForeverForMe/etl.git # 第一次时填写
git push [-u origin master]
```

### 2.1.12 Shell 中 case 命令
```shell
# case 第一种实现方式
# :b:d:i:s:f: 其中开头的冒号是在有选项，但是没有参数时防止报错;参数后的冒号代表这个选项必须有参数
# 索引 $OPTIND
while getopts :b:d:i:s:f: opt; do
  # getopts 在第二次调用时不匹配选项，其他参数也出错。因为OPTIND初始化时为1，改变后不会自动重新赋值
  OPTIND=1
  case $opt in
    b) base_time="$OPTARG" ;;
    d) date_format="$OPTARG" ;;
    i) date_diff="$OPTARG" ;;
    s) secon_arg="$OPTARG" ;;
    f) format="$OPTARG" ;;
    :) echo "请添加参数: -$OPTARG" ;;
    ?) echo "选项未设置: -$OPTARG" ;;
    *) echo "未知情况" ;;
  esac
done

# case 第二种实现方式
while true; do
  # getopts 在第二次调用时不匹配选项，其他参数也出错。因为OPTIND初始化时为1，改变后不会自动重新赋值
  OPTIND=1
  if [ $# == 0 ]; then
    break
  elif [[ $#%2 -eq 0 ]]; then
    # $OPTIND    特殊变量，option index，会逐个递增, 初始值为1 配合getopts使用
    # $OPTARG    特殊变量，option argument，不同情况下有不同的值 配合getopts使用
    case $1 in
      '-aa' ) echo '参数是：'$2; echo '这是第 1 个匹配项'; shift 2;;
      '-ba' ) echo '参数是：'$2; echo '这是第 2 个匹配项'; shift 2;;
      '-ca' ) echo '参数是：'$2; echo '这是第 3 个匹配项'; shift 2;;
      '-da' ) echo '参数是：'$2; echo '这是第 4 个匹配项'; shift 2;;
      ? ) echo '参数是：'$2; echo '匹配项为 ? ： ? ‘问号’的作用是匹配一个字符'; shift 2;;
      * ) echo '参数是：'$2; echo '匹配项为 * ： * ‘星号’的作用是匹配0个或多个字符'; exit
    esac
  else
    echo "输入有误"
    break
  fi
done

# 固定选项
#!/bin/bash
echo "a is 5 ,b is 3. Please select your method: "

a=5
b=3

select var in "a+b" "a-b" "a*b" "a/b"; do
  break
done

case $var in
  "a+b")  echo 'a+b= '`expr $a + $b`;;
  "a-b")  echo 'a-b= '`expr $a - $b`;;
  "a*b")  echo 'a*b= '`expr $a \* $b`;;
  "a/b")  echo 'a/b= '`expr $a / $b`;;
      *)  echo "input error"
esac

# 运行输出
a is 5 ,b is 3. Please select your method:
1) a+b
2) a-b
3) a*b
4) a/b
#? 1
a+b= 8
```

### 2.1.13 Shell 中 ftp 命令
```shell
# -n 不受.netrc文件的影响(ftp默认为读取.netrc文件中的设定)
# -v 显示远程服务器相应信息
# ftp自动登录批量下载文件。
# eof只是一个分界符标志,也可以使用EOM,!等
ftp -n  <<  eof
open 192.168.1.171
user guest 123456
binary                # 文件传输类型
cd /home/data         # cd是在远程主机目录操作的命令
lcd /home/databackup  # lcd是在本地主机目录操作的命令
prompt                # 取消交互
mget *                # mget是批量的下载文件
close
bye
eof

# ftp自动登录批量上传文件
ftp -n  <<  eof
open 192.168.1.171
user guest 123456
binary
hash
cd /home/data
lcd /home/databackup
prompt
mput *
close
bye
eof

# ftp自动登录下载单个文件
ftp -n << eof
open 192.168.1.171
user guest 123456
binary
cd /home/data
lcd /home/databackup
prompt
get a.sh a.sh
close
bye
eof

# ftp自动登录上传单个文件
ftp -n << eof
open 192.168.1.171
user guest 123456
binary
cd /home/data
lcd /home/databackup
prompt
put a.sh a.sh
close
bye
eof
```

### 2.1.14 Linux 中 发送信息到微信
```shell
# 发送信息到微信
#!/bin/sh

expireTime=7200

dbFile="db.json"

corpid=xxx
corpsecret=xxx

touser="xxx"
toparty="xxx"
agentid="xxx"
content="服务器快崩了，你还在这里吟诗作对？"

# s 为秒，m 为 分钟，h 为小时，d 为日数
interval=1s

## 发送报警信息
sendMsg(){
  if [ ! -f "$dbFile" ];then
    touch "$dbFile"
  fi

  # 获取token
  req_time=`jq '.req_time' $dbFile`
  current_time=$(date +%s)
  refresh=false
  if [ ! -n "$req_time" ];then
    refresh=true
  else
    if [ $((current_time-req_time)) -gt $expireTime ];then
      refresh=true
    fi
  fi
  if $refresh ;then
    req_access_token_url=https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=$corpid\&corpsecret=$corpsecret
    access_res=$(curl -s -G $req_access_token_url | jq -r '.access_token')

    # 保存文件
    echo "" > $dbFile
    echo -e "{" > $dbFile
    echo -e "\t\"access_token\":\"$access_res\"," >> $dbFile
    echo -e "\t\"req_time\":$current_time" >> $dbFile
    echo -e "}" >> $dbFile

    echo ">>>刷新Token成功<<<"
  fi

  ## 发送消息
  msg_body="{\"touser\":\"$touser\",\"toparty\":\"$toparty\",\"msgtype\":\"text\",\"agentid\":$agentid,\"text\":{\"content\":\"$content\"}}"
  access_token=`jq -r '.access_token' $dbFile`
  req_send_msg_url=https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=$access_token
  req_msg=$(curl -s -H "Content-Type: application/json" -X POST -d $msg_body $req_send_msg_url | jq -r '.errmsg')

  echo "触发报警发送动作，返回信息为：" $req_msg

}


loopMonitor(){
  echo 'loop'
  flag=`uptime | awk '{printf "%.2f\n", $11 "\n"}'`

  # 0.7 这个阈值可以视情况而定，如cpu核数为n，则可以设置为0.7 * n  具体视情况而定
  c=$(echo "$flag > 0.7" | bc)

  echo ">>>>>>>>>>>>>>>>>>`date`<<<<<<<<<<<<<<<<<<"
  free -m | awk 'NR==2{printf "Memory Usage: %s/%sMB (%.2f%%)\n", $3,$2,$3*100/$2 }'
  df -h | awk '$NF=="/"{printf "Disk Usage: %d/%dGB (%s)\n", $3,$2,$5}'
  uptime | awk '{printf "CPU Load: %.2f\n", $11 "\n"}'
  echo ">>>>>>>>>>>>>>>>>>end<<<<<<<<<<<<<<<<<<"

  if [ $c -eq 1  ];then
    sendMsg
  fi
}


while true; do
  loopMonitor
  sleep $interval
done
```




## 2.2 Kafka 命令
```shell
# 生产者
kafka-console-producer --broker-list bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092 --topic test

# 消费者
kafka-console-consumer --bootstrap-server bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092 --topic test

```


## 2.3 MySQL 命令
```shell
# 连接 MySQL 客户端
mysql -h10.80.176.22 -umeta azkaban -pmeta2015
# 执行 sql 文件
mysql -h10.83.96.7 -uroot hive -p < hive_hive.sql
# 备份 MySQL 表及数据
mysqldump -h10.83.96.7 -uroot weshare -p > hive_weshare.sql
# 直接运行 sql
mysql -h10.80.16.9 -P3306 -uroot -p'!mAkJTMI%lH5ONDw' -Decasdb -s -N -e 'select ORIGINAL_MSG from ecas_msg_log limit 1;'
```


## 2.4 Hive 命令
```shell
# 使用hiveserver2时，需要在core-site.xml中添加hadoop的用户识别
<property>
  <name>hadoop.proxyuser.hadoop.hosts</name>
  <value>*</value>
</property>
<property>
  <name>hadoop.proxyuser.hadoop.groups</name>
  <value>*</value>
</property>

# hiveserver2 的启动命令
nohup /home/hadoop/hive-2.3.5/bin/hiveserver2 &>/dev/null &

# 初始化 Hive 数据库
/home/hadoop/hive-2.3.5/bin/schematool -initSchema -dbType mysql

# 启动 beeline 客户端，连接 hiveserver2 服务
beeline -u jdbc:hive2://node47:10000 -n hive --showHeader=false --outputformat=csv2 -e ''
```


## 2.5 Hadoop 命令及配置
```shell
# 单独启动nodemanager
./hadoop-2.7.7/sbin/yarn-daemon.sh start nodemanager
```


## 2.6 Hadoop 相关下载地址
```shell
# Ambari 相关下载
# Ambari 服务端
http://public-repo-1.hortonworks.com/ambari/centos7/2.x/updates/2.7.3.0/ambari-2.7.3.0-centos7.tar.gz
# HDP
http://public-repo-1.hortonworks.com/HDP/centos7/3.x/updates/3.1.0.0/HDP-3.1.0.0-centos7-rpm.tar.gz
# HDP-UTILS
http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.22/repos/centos7/HDP-UTILS-1.1.0.22-centos7.tar.gz
```


## 2.7 MongoDB 操作
```shell
# 连接 MongoDB 客户端
./mongo 10.80.16.34:27017/admin -u readuser -p G2Vw38JZHeWvM2
# MongoDB 导出 CSV     -d 数据库 -c 数据表 -o 输出路径 -f 字段(csv时必须指定-f)
mongoexport -h 10.83.16.26:27017 -u mongouser -p 6xVMjclL5DSGJPZ -d starsource -c 'ACQUISITION_PLAN' --type=csv -f -o mdb1-examplenet.csv
# MongoDB 导入 CSV
mongoimport --csv -d "baiduled" -c "dataCollection" -o aaa.csv
```


## 2.8 Sqoop 操作
```shell
# 执行SQL语句
sqoop eval \
--connect jdbc:mysql://10.80.16.7:3306/starsource \
--username BDUser_R --password xy@Eh93AmnCkMbiU \
-e 'select * from ORG_INFO'

# 获取数据库名
sqoop list-databases \
--connect jdbc:mysql://10.83.16.32:3306 \
--username bgp_admin --password 3Mt%JjE#WJIt

# 复制表结构到 Hive 中
sqoop create-hive-table \
--connect jdbc:mysql://10.80.16.7:3306/starsource \
--table ORG_INFO \
--username BDUser_R --password xy@Eh93AmnCkMbiU \
--hive-database ods_source_old \
--hive-table ORG_INFO

# 向 Hive 中导数据
sqoop import \
-m 1 \
--connect jdbc:mysql://10.80.16.7:3306/starsource \
--table ORG_INFO \
--username BDUser_R --password xy@Eh93AmnCkMbiU \
--hive-import \
--hive-database ods_source_old \
--hive-table ORG_INFO \
--hive-partition-key \
--hive-partition-value \
--hive-overwrite \
--as-parquetfile \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
-z \
--direct \
--fields-terminated-by '\t'
```


## 2. Excel 操作
```shell
# excel的十字光标
Private Sub Workbook_SheetSelectionChange(ByVal Sh As Object, ByVal Target As Range)
    Cells.Interior.ColorIndex = xlNone
    Rows(Target.Row).Interior.Color = RGB(0,255,255)
    Columns(Target.Column).Interior.Color = RGB(0,255,255)
End Sub
# ODBC连接字符串
Driver={MySQL ODBC 8.0 Unicode Driver};server:10.10.18.48;database=dm_cf;
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
show functions like '*add*';
desc function extended date_add;

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
add jar hdfs:///user/hive/auxlib/qubole-hive-JDBC-0.0.7.jar;
CREATE TEMPORARY EXTERNAL TABLE hive_meta(
  `CD_ID`       decimal(20,0),
  `COMMENT`     string,
  `COLUMN_NAME` string,
  `TYPE_NAME`   string,
  `integer_idx` decimal(11,0)
)
-- STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
-- STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JDBCStorageHandler'
STORED BY 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.16.75/test",
  "hive.sql.dbcp.username"  = "bgp_admin",
  "hive.sql.dbcp.password"  = "3Mt%JjE#WJIt",
  "hive.sql.table"          = "cm_hive",
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

| 操作名称 |                    快捷键                     |                                                  代码                                                  |                                                 结果                                                 |
|----------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| 插入图片 | Shift + Win + K                               | `<img src="file://D:\soft\desktop\8512a353f8a72ff0565d187592880ef.jpg" alt="图片" style="zoom:10%;"/>` | <img src="file://D:\soft\desktop\8512a353f8a72ff0565d187592880ef.jpg" alt="图片" style="zoom:15%;"/> |
| 超链接   | Ctrl + Alt + V                                | `[链接](http://www.baidu.com)`                                                                         | [链接](http://www.baidu.com)                                                                         |
| 引用     | Ctrl + Alt + R 点击快捷键后，直接输入文字即可 | `[引用][引用]  [引用]:http://www.baidu.com`                                                            | [引用][引用]  [引用]:http://www.baidu.com                                                            |
| 插入注释 | Alt + Shift + 6                               | `注释引用[^1]  [^1]: http://www.baidu.com`                                                             | 注释引用[^1]  [^1]: http://www.baidu.com                                                             |
| 加粗文本 |                                               | `**加粗文本** __加粗文本__`                                                                            | **加粗文本** __加粗文本__                                                                            |
| 标记文本 |                                               | `==标记文本==`                                                                                         | ==标记文本==                                                                                         |
| 删除文本 |                                               | `~~删除文本~~`                                                                                         | ~~删除文本~~                                                                                         |
| 引用文本 |                                               | `> 引用文本`                                                                                           | > 引用文本                                                                                           |
| 下标     |                                               | `H~2~O is是液体。`                                                                                     | H~2~O is是液体。                                                                                     |
| 幂运算   |                                               | `2^10^ 运算结果是 1024`                                                                                | 2^10^ 运算结果是 1024                                                                                |
