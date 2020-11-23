[TOC]
# 1、公司级信息
## 1.1 服务器
### 1.1.1 生产
|      系统     |  服务 |                    ip或网址                    |  用户 |     密码     |    备注   |
|---------------|-------|------------------------------------------------|-------|--------------|-----------|
| linux         | ftp   | 10.90.0.5                                      |       |              | FTP       |
| node47        | linux | 10.80.1.47                                     | root  | CQBP53G(Lv82 | 大数据 新 |
| node148       | linux | 10.80.1.148                                    | root  | CQBP53G(Lv82 | 大数据 新 |
| node172       | linux | 10.80.1.172                                    | root  | CQBP53G(Lv82 | 大数据 新 |
| bsprd-hadoop1 | linux | 10.80.0.20                                     | root  | Xfx2018@)!*  | 大数据 旧 |
| bsprd-hadoop2 | linux | 10.80.0.23                                     | root  | Xfx2018@)!*  | 大数据 旧 |
| bsprd-hadoop3 | linux | 10.80.0.29                                     | root  | Xfx2018@)!*  | 大数据 旧 |
| cm6           | web   | http://10.80.1.47:7180/cmf/home                | admin | admin        |           |
| cm5           | web   | http://10.80.0.20:7180/cmf/home                | admin | admin        |           |
| hue6          | web   | http://10.80.1.47:8889/hue/editor/?type=impala | admin | dFGYXpxifv   |           |
| hue5          | web   | http://10.80.0.20:8889/hue/editor/?type=impala | admin | admin        |           |
| streamsets    | web   | http://10.80.1.172:18630/                      | admin | admin        |           |

### 1.1.2 测试
|     系统    |  作用 |                    ip或网址                     |  用户  |       密码       |    备注   |
|-------------|-------|-------------------------------------------------|--------|------------------|-----------|
| linux       | ftp   | 10.83.0.32                                      | it-dev | 058417gv         |           |
| node47      | linux | 10.83.0.47                                      | root   | (Ob!)Y#G3Anf     | 大数据 新 |
| node123     | linux | 10.83.0.123                                     | root   | (Ob!)Y#G3Anf     | 大数据 新 |
| node129     | linux | 10.83.0.129                                     | root   | (Ob!)Y#G3Anf     | 大数据 新 |
| bssit-cdh-1 | linux | 10.83.80.5                                      | root   | !W$WdwY7U%pe)YkQ | 大数据 旧 |
| bssit-cdh-2 | linux | 10.83.80.7                                      | root   | !W$WdwY7U%pe)YkQ | 大数据 旧 |
| bssit-cdh-3 | linux | 10.83.80.14                                     | root   | !W$WdwY7U%pe)YkQ | 大数据 旧 |
| bssit-cdh-4 | linux | 10.83.80.2                                      | root   | !W$WdwY7U%pe)YkQ | 大数据 旧 |
| cm6         | web   | http://10.83.0.47:7180/cmf/home                 | admin  | admin            |           |
| cm5         | web   | http://10.83.80.5:7180/cmf/home                 | admin  | admin            |           |
| hue6        | web   | http://10.83.0.123:8889/hue/editor/?type=impala | admin  | admin            |           |
| hue5        | web   | http://10.83.80.5:8889/hue/editor/?type=impala  | admin  | admin            |           |
| streamsets  | web   | http://10.83.0.129:18630                        | admin  | admin            |           |


## 1.2 数据库配置
### 1.2.1 生产
| 系统 |      ip     |   username  |     password     |   备注  |
|------|-------------|-------------|------------------|---------|
| 星连 | 10.80.16.5  | root        | Xfx2018@)!*      |         |
| 星云 | 10.80.16.21 | root        | EXYeaGVQZpsr@CR& |         |
| 风控 | 10.80.16.42 | root        | 8x3V1lrbkS       | 旧      |
| 风控 | 10.80.16.65 | selectuser  | Risk_Reader001   | 新      |
| 核心 | 10.80.16.9  | root        | !mAkJTMI%lH5ONDw | 旧      |
| 核心 | 10.80.16.10 | root        | LZWkT2lxze6x%1V( | 新      |
| 核心 | 10.80.16.25 | root        | LZWkT2lxze6x%1V( | 回放    |
| 催收 | 10.80.16.87 | root        | wH7Emvsrg&V5     |         |
| H5   | 10.80.16.73 | UeserReader | Ws2019!@         |         |
| CM5  | 10.80.16.3  | root        | Xfx2018@)!*      |         |
| CM6  | 10.80.16.75 | bgp_admin   | U3$AHfp*a8M&     | MariaDB |

### 1.2.2 测试
| 系统 |      ip     |   username  |     password     |   备注  |
|------|-------------|-------------|------------------|---------|
| 星连 | 10.83.16.10 | root        | Xfxcj2018@)!*    |         |
| 星云 | 10.83.16.15 | root        | Ws2018!07@       |         |
| 风控 | 10.83.16.9  | root        | Xfx2018@)!*      |         |
| 核心 | 10.83.16.16 | root        | Zn9nvRr9gw1pugP  | 旧1     |
| 核心 | 10.83.16.42 | root        | 7H*qb0etoNEfvAtM | 旧2     |
| 催收 | 10.83.16.23 | root        | Ws2018!07@       | 旧      |
| 核心 | 10.83.16.43 | root        | zU!ykpx3EG)$$1e6 | 新$$    |
| H5   | 10.83.16.33 | UeserReader | Ws2019!@         |         |
| CM5  | 10.83.96.10 | root        | !W$WdwY7U%pe)YkQ |         |
| CM6  | 10.83.16.32 | bgp_admin   | 3Mt%JjE#WJIt     | MariaDB |

### 1.2.3 UAT
| 系统 |      ip     | username |     password     | 备注 |
|------|-------------|----------|------------------|------|
| 核心 | 10.83.16.18 | root     | fzh6M#fmu3Rr7MTi |      |

## [1.3 资金方 - 项目 - 产品 码表](https://docs.qq.com/sheet/DRVpEWmtVZHdKWm5l)


## 1.4 公司邮箱
| 环境 |          邮箱IP         | 端口 |                  账户                  |       密码       |
|------|-------------------------|------|----------------------------------------|------------------|
| 生产 | https://10.80.0.133/owa |   25 | apb-report@service.weshareholdings.com | &kQ4TOWerGlfpUm7 |
| 测试 | https://10.83.0.44/owa  |   25 | apb-report@weshareholdings.com.cn      | Ws2018!08@       |



## 1.5 Davinci 账号密码
| 名称 |               属性              | 备注 |
|------|---------------------------------|------|
| 地址 | http://10.80.1.172:8097/#/login |      |
| 用户 | davinci/FKD4WKmQpdJ#:apjfAPn    |      |
| 用户 | weshare/ZmG!TN+_8hWuUa@s        |      |
| 用户 | yunan.huang/IBxkYU2N            |      |
| 用户 | weiximing/000000                |      |


# 2、命令脚本操作
## 2.1 Shell 命令
### 2.1.1 基础 Shell 命令
```shell
#!/bin/bash -e

. /etc/profile
. ~/.bash_profile
export LANG=zh_CN.UTF-8

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
${#array[@]} 获取数组元素的个数
${!array[@]} 获取数组元素的 Key 值
${array[@]}  获取数组元素的 Value 值
declare -A 获取 Map 类型的元素
# 添加 Map 值
map["300"]="3"
# 输出 Map key 对应的值
echo ${map["100"]}
# 遍历  Map
for key in ${!map[@]};do echo ${map[$key]}; done
```

### 2.1.4 Shell 中判断选项的用法
```shell
# 判断时各选项的用法
-a  逻辑与
-o  逻辑或
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
echo -e '\033[30m -- \33[37m设置前景色黑,红,绿,棕[黄],蓝,紫,青,白\033[0m'
echo -e '\033[40m -- \33[47m设置背景色黑,红,绿,棕[黄],蓝,紫,青,白\033[0m'
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
-a 不要忽略二进制数据。
-A<显示列数> 除了显示符合范本样式的那一行之外，并显示该行之后的内容。
-b 在显示符合范本样式的那一行之外，并显示该行之前的内容。
-c 计算符合范本样式的列数。
-C<显示列数>或-<显示列数>  除了显示符合范本样式的那一列之外，并显示该列之前后的内容。
-d<进行动作> 当指定要查找的是目录而非文件时，必须使用这项参数，否则grep命令将回报信息并停止动作。
-e<范本样式> 指定字符串作为查找文件内容的范本样式。
-E 将范本样式为延伸的普通表示法来使用，意味着使用能使用扩展正则表达式。
-f<范本文件> 指定范本文件，其内容有一个或多个范本样式，让grep查找符合范本条件的文件内容，格式为每一列的范本样式。
-F 将范本样式视为固定字符串的列表。
-G 将范本样式视为普通的表示法来使用。
-h 在显示符合范本样式的那一列之前，不标示该列所属的文件名称。
-H 在显示符合范本样式的那一列之前，标示该列的文件名称。
-i 忽略字符大小写的差别。
-l 列出文件内容符合指定的范本样式的文件名称。
-L 列出文件内容不符合指定的范本样式的文件名称。
-n 在显示符合范本样式的那一列之前，标示出该列的编号。
-q 不显示任何信息。
-R/-r 此参数的效果和指定“-d recurse”参数相同。
-s 不显示错误信息。
-v 反转查找。
-w 只显示全字符合的列。
-x 只显示全列符合的列。
-y 此参数效果跟“-i”相同。
-o 只输出文件中匹配到的部分。

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

### 2.1.14 Shell 中 awk 命令
```shell
echo -ne '
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | addition_overview.hql          | 1546272000  | 1546272000  | -
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | adt_admin.hql                  | 1546272000  | 1546272000  | -
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | adt_data.hql                   | 1546272000  | 1546272000  | -
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | advertising_space.hql          | 1546272000  | 1546272000  | -
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | data_preference.hql            | 1546272000  | 1546272000  | -
execut  | beeline  | spark                     | hdfs       | 10000            | -                              | dm_cf       | retention_overview.hql         | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | addition_overview              | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | adt_admin                      | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | adt_data                       | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | advertising_space              | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | data_preference                | 1546272000  | 1546272000  | -
export  | mysql    | mysql22                   | root       | INikGPLun*8v     | dm_cf                          | microb      | retention_overview             | 1546272000  | 1546272000  | -
import  | local    | app41                     | -          | -                | /app_home/logs/ads             | ods_wefix   | access.x.log                   | 1546272000  | 1546272000  | 20190101
import  | local    | app41                     | -          | -                | /app_home/logs/ads             | ods_wefix   | bi.x.log                       | 1546272000  | 1546272000  | 20190101
import  | local    | app41                     | -          | -                | /app_home/logs/strategy        | ods_wefix   | atd_black.x.log                | 1546272000  | 1546272000  | 20190101
import  | local    | app41                     | -          | -                | /app_home/logs/strategy        | ods_wefix   | atd_device.x.log               | 1546272000  | 1546272000  | 20190101
import  | local    | app41                     | -          | -                | /app_home/logs/strategy        | ods_wefix   | atd_ip.x.log                   | 1546272000  | 1546272000  | 20190101
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | CLIENT_INFO                    | 1546272000  | 1546272000  | -
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | EVENT_LOGGER                   | 1546272000  | 1546272000  | 20190101
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | FLOW_RECORD                    | 1546272000  | 1546272000  | 20190101
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | PRODUCT_INFO                   | 1546272000  | 1546272000  | -
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | RECOMMEND_FLOW                 | 1546272000  | 1546272000  | 20190101
import  | mongodb  | mongo26                   | mongouser  | 6xVMjclL5DSGJPZ  | starsource                     | ods_source  | SOURCE_INFO                    | 1546272000  | 1546272000  | -
import  | mysql    | mysql07                   | root       | RRDdjhPULOdZ703  | app_builder_um                 | ods_source  | TENANT                         | 1546272000  | 1546272000  | -
import  | mysql    | mysql22                   | root       | INikGPLun*8v     | microb                         | ods_wefix   | ACQUISITION_PLAN               | 1546272000  | 1546272000  | -
import  | mysql    | mysql22                   | root       | INikGPLun*8v     | microb                         | ods_wefix   | ADVERTISEMENT_INFO             | 1546272000  | 1546272000  | -
import  | mysql    | mysql22                   | root       | INikGPLun*8v     | microb                         | ods_wefix   | APP_INFO                       | 1546272000  | 1546272000  | -
import  | mysql    | mysql22                   | root       | INikGPLun*8v     | microb                         | ods_wefix   | EXCHANGE_INFO                  | 1546272000  | 1546272000  | -
import  | mysql    | mysql22                   | root       | INikGPLun*8v     | microb                         | ods_wefix   | EXCHANGE_INFO_CHILD            | 1546272000  | 1546272000  | -
' | grep -vE '^\s*$' | awk -F '[| ]*' '{
  print_format="%-7s | %-8s | %-25s | %-10s | %-16s | %-30s | %-11s | %-30s | %-11s | %-11s | %s\n"
  if($1 == "import" && $2 == "mysql" && $7 == "ods_source" && $8 == "TENANT"){
    printf "\033[32m%-7s | %-8s | %-25s | %-10s | %-16s | %-30s | %-11s | %-30s | %-11s | %-11s | %-8s | 原值\033[0m\n",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
    $8 = "aa"
    printf "\033[33m%-7s | %-8s | %-25s | %-10s | %-16s | %-30s | %-11s | %-30s | %-11s | %-11s | %-8s | 新值\033[0m\n",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
  }
  printf print_format,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
  # > "./conf/imex.table"
}'
```


### 2.1.15 Linux 中 发送信息到微信
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

### 2.1.16 Shell 中 find 命令
```shell
# 批量查找文件并删除
find . -name 'data_log.log.2020-09-2*' -type f -mtime +1 | xargs rm
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

# 启动 beeline 客户端，连接 hiveserver2 服务，导出数据
beeline -u jdbc:hive2://node47:10000 -n hive --showHeader=false --outputformat=csv2 -e ''
# 连接 beeline 客户端
beeline -n hive -u jdbc:hive2://node47:10000 --color=true --hiveconf hive.resultset.use.unique.column.names=false
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

# 向 Hive 中导数据    -m 指定 maptask 任务数
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


## 2.9 Janusgraph 操作
```java
// 启动 sql形式
export HADOOP_CONF_DIR=/usr/hdp/3.1.0.0-78/hadoop/etc/hadoop
export CLASSPATH=$HADOOP_CONF_DIR
/home/hdfs/janusgraph-0.4.0/bin/gremlin.sh -i /home/hdfs/janusgraph-0.4.0/northwind.groovy

./bin/gremlin.sh -i northwind.groovy

// 创建 sql形式
graph = NorthwindFactory.createGraph()
g = graph.traversal()

// 远程连接
:remote connect tinkerpop.server conf/remote.yaml
:remote console

// 应用于 Gephi
:plugin use tinkerpop.gephi
:remote connect tinkerpop.gephi
:remote config host 10.10.18.48
:remote config workspace janusgraph
graph = TinkerFactory.createModern()
:> graph

// 显示变量
:show variables

// 众神之图
// 入口
:plugin use tinkerpop.hadoop
:plugin use tinkerpop.spark

graph = GraphFactory.open('conf/hadoop-graph/spark-hbase-es.proper')
g = graph.traversal().withComputer(SparkGraphComputer)

g.V().count()

g.E().count()

:remote connect tinkerpop.hadoop graph g
:> g.V().group().by{it.value('name')[1]}.by('name')

// 加载众神之图
graph = JanusGraphFactory.open('conf/http-janusgraph-hbase-es.properties')

GraphOfTheGodsFactory.load(graph)

g = graph.traversal()

// 点查询
g.V()
v[4128]
v[4152]
v[8248]
v[12344]
v[4184]
v[4248]
v[8344]
v[4288]
v[8384]
v[12480]
v[16576]
v[20672]

// 显示50个节点的详细信息
g.V().valueMap(true).limit(50)
// 显示前50个节点name属性
g.V().values('name').limit(50)
// 会将属性显示出来
g.V().properties('name').limit(50)
// 根据系统id：1234 查询name属性，可以直接 .属性() 查询
g.V('1234').values('name') // = g.V().name()

// 边查询
g.E()
e[5639][12344-lives->8344]
e[2571][4184-lives->8248]
e[3083][4184-brother->4288]
e[3595][4184-brother->12480]
e[10264][4288-lives->4128]
e[9752][4288-father->4152]
e[10776][4288-brother->4184]
e[11288][4288-brother->12480]
e[12312][8384-mother->4248]
e[11800][8384-father->4288]
e[13848][8384-battled->12344]
e[12824][8384-battled->16576]
e[13336][8384-battled->20672]
e[15384][12480-lives->8344]
e[14872][12480-brother->4184]
e[14360][12480-brother->4288]
e[15896][12480-pet->12344]

// 显示50个节点的详细信息
g.E().valueMap(true).limit(50)
// properties与values基本相同，但是properties更详细
g.E().properties().limit(50)
g.E().values().limit(50)
// 查询label
g.E().label().limit(50)

// 获取 顶点id 为 12344 的边为 lives 的边信息
g.V(12344).outE('lives').valueMap(true)
[id:5639,label:lives]
g.V(12344).outE('lives')
e[5639][12344-lives->8344]

// 获取 边为 lives 的边信息
g.V().outE('lives')
e[5639][12344-lives->8344]
e[2571][4184-lives->8248]
e[10264][4288-lives->4128]
e[15384][12480-lives->8344]

g.V(12344).outE('lives').otherV()
g.V(12344).out('lives').values('name')

g.V('20672').outE().otherV().limit(50).valueMap(true)

g.V().valueMap(true).limit(50)

g.V('20672').valueMap(true)

g.V('4248').outE().otherV().valueMap(true)

// 自定义三国杀
// 创建入口
graph = HadoopGraph.open('conf/hadoop-graph/spark-hbase-es.proper')
graph = JanusGraphFactory.open('conf/http-janusgraph-hbase-es.properties')

// 创建顶点标签
mgmt = graph.openManagement()
mgmt.makeVertexLabel('person').make()
mgmt.makeVertexLabel('country').make()
mgmt.makeVertexLabel('weapon').make()
mgmt.getVertexLabels()
mgmt.commit()

// 创建边标签
mgmt = graph.openManagement()
brother = mgmt.makeEdgeLabel("brother").make()
mgmt.makeEdgeLabel("battled").make()
mgmt.makeEdgeLabel("belongs").make()
mgmt.makeEdgeLabel("use").make()
mgmt.getRelationTypes(EdgeLabel.class)
mgmt.commit()

// 创建属性
mgmt = graph.openManagement()
name = mgmt.makePropertyKey('name').dataType(String.class).cardinality(Cardinality.SET).make()
mgmt.buildIndex('nameUnique', Vertex.class).addKey(name).unique().buildCompositeIndex()
age = mgmt.makePropertyKey("age").dataType(Integer.class).make()
mgmt.buildIndex('age2', Vertex.class).addKey(age).buildMixedIndex("janusgraph")
mgmt.getGraphIndexes(Vertex.class)
mgmt.commit()

// 添加顶点
g = graph.traversal()

// liubei =
g.addV('person').property('name','刘备').property('age',45)
// guanyu =
g.addV("person").property('name','关羽').property('age',42)
// zhangfei =
g.addV("person").property('name','张飞').property('age',40)
// lvbu =
g.addV("person").property('name','吕布').property('age',38)
g.addV("country").property('name','蜀国')
g.addV("weapon").property('name','方天画戟')
g.addV("weapon").property('name','双股剑')
g.addV("weapon").property('name','青龙偃月刀')
g.addV("weapon").property('name','丈八蛇矛')

for (tx in graph.getOpenTransactions()) tx.commit()

// 添加关系
g.addE('brother').from(g.V(4112)).to(g.V(8208))
g.addE('brother').from(g.V(4112)).to(g.V(4280))
g.addE('brother').from(g.V(4280)).to(g.V(4112))
g.addE('brother').from(g.V(8208)).to(g.V(4112))
g.addE('brother').from(g.V(8208)).to(g.V(4280))
g.addE('brother').from(g.V(4280)).to(g.V(8208))

g.addE('use').from(g.V(4112)).to(g.V(4312))
g.addE('use').from(g.V(4280)).to(g.V(4320))
g.addE('use').from(g.V(8208)).to(g.V(4152))
g.addE('use').from(g.V(4264)).to(g.V(4160))

g.addE('belongs').from(g.V(4112)).to(g.V(8360))
g.addE('belongs').from(g.V(4280)).to(g.V(8360))
g.addE('belongs').from(g.V(8208)).to(g.V(8360))

g.addE('battled').from(g.V(4264)).to(g.V(4112))
g.addE('battled').from(g.V(4264)).to(g.V(4280))
g.addE('battled').from(g.V(4264)).to(g.V(8208))
g.addE('battled').from(g.V(4112)).to(g.V(4264))
g.addE('battled').from(g.V(4280)).to(g.V(4264))
g.addE('battled').from(g.V(8208)).to(g.V(4264))

for (tx in graph.getOpenTransactions()) tx.commit()


// gremlin查看顶点数和关系数 9 19
g.V().count()
g.E().count()


// 查询刘备的兄弟
g.V().has('name','刘备').next() // 返回 ==>v[4112]
g.V(4112).out('brother').values()

// 查询蜀国的所有人物
g.V().has('name','蜀国').next() // 返回 ==>v[8360]
g.V(8360).in('belongs').valueMap()
```





# 3、Python 脚本操作

## 3.1 邮件发送
### 邮件主类
```python
#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import re
import sys
from configparser import ConfigParser
from email.mime.text import MIMEText
from email.header import Header
from smtplib import SMTP


class Mail(object):
    def __init__(self, path):
        self.charset = 'utf-8'
        self.str_join = ','
        self.end_with_mail = 'qq.com'
        self.sender_name = 'sender'
        self.host_name = 'host'
        self.user_name = 'user'
        self.pass_name = 'pass'
        self.receiver_name = 'receiver'
        self.receivers_name = 'receivers'
        self.sender_mail = 'sender_mail'
        self.receivers_mail = 'receivers_mail'
        self.config = ConfigParser(allow_no_value=True)  # 创建对象
        self.config.read(path, encoding=self.charset)
        self.server = None

    def sender(self, host, user, password):
        if not self.config.has_section(self.sender_name):  # 检查指定节点是否存在，如果不存在则创建
            self.config.add_section(self.sender_name)

        if not self.config.has_option(self.sender_name, self.host_name):
            self.config.set(self.sender_name, self.host_name, host)

        if not self.config.has_option(self.sender_name, self.user_name):
            self.config.set(self.sender_name, self.user_name, user)

        if not self.config.has_option(self.sender_name, self.pass_name):
            self.config.set(self.sender_name, self.pass_name, password)

        __mail__ = self.config.get(self.sender_name, self.user_name)
        self.config.set(self.sender_name, self.sender_mail, re.findall(r'<(.*?)>', __mail__)[0])

    def receivers(self, receiver_all):
        if not self.config.has_section(self.receiver_name):  # 检查指定节点是否存在，如果不存在则创建
            self.config.add_section(self.receiver_name)

        if not self.config.has_option(self.receiver_name, self.receivers_name):
            self.config.set(self.receiver_name, self.receivers_name, receiver_all)
            self.config.set(self.receiver_name, self.receivers_mail, re.findall(r'<(.*?)>', receiver_all))

        __mails__ = self.config.get(self.receiver_name, self.receivers_name)
        self.config.set(self.receiver_name, self.receivers_mail, self.str_join.join(re.findall(r'<(.*?)>', __mails__)))

    def send_mail(self, sub, msg):
        """ 设置邮件消息 """
        mail_msg = MIMEText(message, 'plain', _charset=self.charset)  # 根据邮件内容，获取邮件
        mail_msg['From'] = Header(self.config.get(self.sender_name, self.user_name), charset=self.charset)
        mail_msg['To'] = Header(self.config.get(self.receiver_name, self.receivers_name), charset=self.charset)
        mail_msg['Subject'] = Header('{}'.format(sub), charset=self.charset)

        print(mail_msg)
        print(self.config.items(self.sender_name))
        print(self.config.items(self.receiver_name))

        self.server = SMTP(self.config.get(self.sender_name, self.host_name), 25)  # 25 为 SMTP 端口号

        if self.config.get(self.sender_name, self.sender_mail).endswith(self.end_with_mail):
            self.server.login(self.config.get(self.sender_name, self.sender_mail),
                              self.config.get(self.sender_name, self.pass_name))

        self.server.sendmail(self.config.get(self.sender_name, self.user_name),
                             self.config.get(self.receiver_name, self.receivers_mail).split(self.str_join),
                             mail_msg.as_string())  # 发件人、收件人、消息

    def close(self):
        self.server.quit()  # 退出对话
        self.server.close()  # 关闭连接


if __name__ == '__main__':
    """ 获取参数 """
    if len(sys.argv[1:]) != 3:
        print("Error：请指定：配置文件、消息内容、主题", sys.argv)
        sys.exit(1)

    if not os.path.exists(sys.argv[1]):
        print("Error：配置文件 未找到！")
        sys.exit(1)

    """ 属性设置 """
    ini_path = sys.argv[1]
    subject = sys.argv[2]
    message = sys.argv[3]

    send_host = '10.80.0.133'
    send_user = '告警邮箱<DataCenter-Alert@services.weshreholdings.com>'
    send_pass = ''
    receivers = '郭超<chao.guo@weshareholdings.com>,' \
                '檀剑<jian.tan@weshareholdings.com>,' \
                '黄育楠<yunan.huang@weshareholdings.com>,' \
                '刘焕<huan.liu@weshareholdings.com>,' \
                '王禹衡<yuheng.wang@weshareholdings.com>,' \
                '魏喜明<ximing.wei@weshareholdings.com>'

    mail = Mail(ini_path)
    mail.sender(send_host, send_user, send_pass)
    mail.receivers(receivers)
    mail.send_mail(subject, message)
    mail.close()
```
### 配置文件
```ini
[sender]
host = smtp.qq.com
user = 魏喜明<664651151@qq.com>
;pass = qonjgkozycnjbfhg

[receiver]
receivers = 魏喜明<664651151@qq.com>,魏喜明<ximing.wei@weshareholdings.com>
```


# 4、SQL 语句

## 4.0 数仓
### 4.0.1 数仓分层
| 模型层次 |        英文全称        |   中文名   |                                                                                                                                          层次定义                                                                                                                                          |
|----------|------------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ODL      | Operational Data Layer | 操作数据层 | 该层级主要功能是存储从源系统直接获得的数据（数据从数据结构、数据之间的逻辑关系上都与源系统基本保持一致）                                                                                                                                                                                   |
| IDL      | Integrated Data Layer  | 集成数据层 | 该层的主要功能是基于主题域的划分，面向业务主题、以数据为驱动设计模型，并且基于3NF建模，完成数据整合，提供统一的基础数据来源。                                                                                                                                                              |
| CDL      | Component Data Layer   | 元件数据层 | 面向分析主题的、统一的数据访问层，所有的基础数据、业务规则和业务实体的基础指标库以及多维模型都在这里统一计算口径、统一建模，大量基础指标库以及多维模型在该层实现。该层级以分析需求为驱动进行模型设计，实现跨业务主题域数据的关联计算或者轻度汇总计算，因此会有大数据量的多表关联汇总计算。 |
| MDL      | Mart Data Layer        | 数据集市层 | 该层次主要功能是加工多维度冗余的宽表（解决复杂的查询）、多维分析的汇总表。                                                                                                                                                                                                                 |
| ADL      | Application Data Layer | 应用数据层 | 该层级的主要功能是满足业务方的需求；在该层级实现报表（海豚、星空、邮件报表）、自助取数等需求。                                                                                                                                                                                             |
| DIM      | Dimension Data Layer   | 维度数据层 | 该层主要存储简单、静态、代码类的维表，包括从OLTP层抽取转换维表、根据业务或分析需求构建的维表以及仓库技术维表如日期维表等。                                                                                                                                                                 |
| REF      |                        | 数据接口层 | 该层级独立于其他层级之外，其来源可以是任意层级，主要存放的是在与其他组对接的时候提供给对方的数据                                                                                                                                                                                           |

## 4.1 SQL 函数及通用语句
```sql
-- HQL 学习
-- union 与 union all 相比 多了去重排序的功能

-- order by           全局有序，需要指定 hive.mapred.mode=nostrict 非严格模式
-- sort by            Reduce 有序，可指定 mapred.reduce.tasks=n 设置 Reduce 数量
-- distribute by      控制 Map 中的数据如何进入 Reduce
-- cluster by         具有 distribute by 与 sort by 的功能，只能倒叙

-- 查看分区
show partitions ods_wefix.t_ad_query_water_json;

DROP DATABASE IF EXISTS database cascade;      -- 级联删除，即：删除数据库的同时删除库中的表
DROP DATABASE IF EXISTS database restrict;     -- 限制删除，即：删除数据库时有限制，需要先删除库中的表
CREATE DATABASE IF NOT EXISTS dm_report_asset; -- 创建数据库
-- 删除表
DROP TABLE IF EXISTS test;
-- 创建临时表
CREATE TEMPORARY TABLE IF NOT EXISTS test(
  id int COMMENT 'id',
  name string COMMENT '名称'
) COMMENT '测试表'
;

-- 表重命名
ALTER TABLE test RENAME TO tet; ALTER TABLE tet RENAME TO test;
-- 修改表注释
ALTER TABLE test SET TBLPROPERTIES('comment' = '这是表注释!');
-- 添加字段
ALTER TABLE test ADD COLUMNS (t_1 string comment '测试');
-- 修改字段注释
ALTER TABLE test CHANGE COLUMN t_1 t string comment '这里是列注释!';
-- 修改字段
ALTER TABLE test CHANGE COLUMN t_1 t string AFTER id;
-- 删除字段
ALTER TABLE test REPLACE COLUMNS (id int COMMENT 'id', name string COMMENT '名称');
-- 增加分区
ALTER TABLE test ADD IF NOT EXISTS PARTITION (year_month='201911',day_of_month='29');
-- 删除分区
ALTER TABLE test DROP IF EXISTS PARTITION (year_month = '201911',day_of_month = 8);


-- regexp_replace(String A,String B,String C) 替换函数：将字符串 A 中的符合 Java 正则表达式 B 的部分替换为 C
-- space(Int n) 空格字符串函数：返回长度为 n 的字符串
select regexp_replace(space(10),' ','0');
-- 与上个语句等价 repeat(String str, Int n) 重复字符串函数：返回长度为 n 的字符串
select repeat('a',10);

-- 进制转换测试
select
  '中国'                                   as `10 进制`,        -- 中国
  hex('中国')                              as `10 ——> 16 进制`, -- E4B8ADE59BBD
  conv(hex('中国'),16,2)                   as `16 ——> 2  进制`, -- 111001001011100010101101111001011001101110111101
  conv(conv(hex('中国'),16,2),2,16)        as `2  ——> 16 进制`, -- E4B8ADE59BBD
  unhex(conv(conv(hex('中国'),16,2),2,16)) as `16 ——> 10 进制`  -- 中国
;

-- 取多个数之间的最大、最小值  least(v1, v2, ...)  greatest(v1, v2, ...)
select least('2020-07-14',to_date('2020-07-01 10:10:10')) as min,greatest('2020-07-14','2020-07-01 10:10:10') as max; -- 2020-07-01  2020-07-14
select least(null,'2020-07-14',to_date('2020-07-01 10:10:10')) as min,greatest(null,'2020-07-14','2020-07-01 10:10:10') as max; -- null  null

-- 取首个非空值。 nvl(a,b) 如果 a 为空，取 b ，否则取 a。 coalesce(a,b,c,...) 从前往后，取第一个非空值
select
  nvl(null,'nvl_1_a') as nvl_1,
  nvl('nvl_2_a','nvl_2_b') as nvl_2,
  coalesce(null,null,'coalesce_1_a') as coalesce_1,
  coalesce(null,'coalesce_2_a','coalesce_2_b') as coalesce_2,
  coalesce('coalesce_3_a','coalesce_3_b','coalesce_3_c') as coalesce_3
;

-- 月初或年初日期  两个参数：第一个参数是 date 类型：yyyy-MM-dd HH:mm:ss 或 yyyy-MM-dd；第二个参数是字符串类型：MONTH MON MM 或 YEAR YYYY YY
select trunc('2020-12-31','MM'); -- 2020-12-01
```

## 4.2 Hive
### 4.2.1 Hive 学习
#### 4.2.1.1 Hive 基础学习
```sql
-- Numeric Types
--   TINYINT          (1-byte signed integer, from -128                     to 127)
--   SMALLINT         (2-byte signed integer, from -3 2768                  to 3 2767)
--   INT/INTEGER      (4-byte signed integer, from -21 4748 3648            to 21 4748 3647)
--   BIGINT           (8-byte signed integer, from -922 3372 0368 5477 5808 to 922 3372 0368 5477 5807)
--   FLOAT            (4-byte single precision floating point number)             -- 单精度浮点型数值
--   DOUBLE           (8-byte double precision floating point number)             -- 双精度浮点型数值
--   DOUBLE PRECISION (alias for DOUBLE, only available starting with Hive 2.2.0) -- DOUBLE 的别名 -- available 可用的
--   DECIMAL                                                                      -- DECIMAL（精度，刻度）precision 默认 10，scale 默认 0 （即无小数位）
--     Introduced in Hive 0.11.0 with a precision of 38 digits                    -- 在 Hive 0.11.0 中引入，精度为 38 位
--     Hive 0.13.0 introduced user-definable precision and scale                  -- 在 Hive 0.13.0 中引入了用户可定义的精度和比例
--   NUMERIC          (same as DECIMAL, starting with Hive 3.0.0)
-- Date/Time Types （使用 timestamp.formats 来支持其他时间戳格式。例如，yyyy-MM-dd'T'HH:mm:ss.SSS，yyyy-MM-dd'T'HH:mm:ss）
--   TIMESTAMP (Note: Only available starting with Hive 0.8.0)                    -- yyyy-mm-dd hh:mm:ss
--   DATE (Note: Only available starting with Hive 0.12.0)                        -- yyyy­mm­dd
--   INTERVAL (Note: Only available starting with Hive 1.2.0)                     -- 间隔，与NUMERIC不一样。不明白怎么用
-- String Types
--   STRING
--   VARCHAR (Note: Only available starting with Hive 0.12.0)                     -- 可变长度（ 1 和 6 5535 ），如：指定 10 ，不足 10 为本身，超过截前 10
--   CHAR (Note: Only available starting with Hive 0.13.0)                        -- 固定长度值，最长 255
-- Misc Types
--   BOOLEAN
--   BINARY (Note: Only available starting with Hive 0.8.0)
-- Complex Types
--   arrays: ARRAY<data_type> (Note: negative values and non-constant expressions are allowed as of Hive 0.14.)
--   maps: MAP<primitive_type, data_type> (Note: negative values and non-constant expressions are allowed as of Hive 0.14.)
--   structs: STRUCT<col_name : data_type [COMMENT col_comment], ...>
--   union: UNIONTYPE<data_type, data_type, ...> (Note: Only available starting with Hive 0.7.0.)





CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name    -- (Note: TEMPORARY available in Hive 0.14.0 and later)
  [(col_name data_type [column_constraint_specification] [COMMENT col_comment], ... [constraint_specification])]
  [COMMENT table_comment]
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
  [CLUSTERED BY (col_name, col_name, ...) [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS]
  [SKEWED BY (col_name, col_name, ...)                  -- (Note: Available in Hive 0.10.0 and later)]
     ON ((col_value, col_value, ...), (col_value, col_value, ...), ...)
     [STORED AS DIRECTORIES]
  [
   [ROW FORMAT row_format]
   [STORED AS file_format]
     | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]  -- (Note: Available in Hive 0.6.0 and later)
  ]
  [LOCATION hdfs_path]
  [TBLPROPERTIES (property_name=property_value, ...)]   -- (Note: Available in Hive 0.6.0 and later)
  [AS select_statement];   -- (Note: Available in Hive 0.5.0 and later; not supported for external tables)

CREATE [TEMPORARY] [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
  LIKE existing_table_or_view_name
  [LOCATION hdfs_path];

data_type
  : primitive_type
  | array_type
  | map_type
  | struct_type
  | union_type  -- (Note: Available in Hive 0.7.0 and later)

primitive_type
  : TINYINT
  | SMALLINT
  | INT
  | BIGINT
  | BOOLEAN
  | FLOAT
  | DOUBLE
  | DOUBLE PRECISION -- (Note: Available in Hive 2.2.0 and later)
  | STRING
  | BINARY      -- (Note: Available in Hive 0.8.0 and later)
  | TIMESTAMP   -- (Note: Available in Hive 0.8.0 and later)
  | DECIMAL     -- (Note: Available in Hive 0.11.0 and later)
  | DECIMAL(precision, scale)  -- (Note: Available in Hive 0.13.0 and later)
  | DATE        -- (Note: Available in Hive 0.12.0 and later)
  | VARCHAR     -- (Note: Available in Hive 0.12.0 and later)
  | CHAR        -- (Note: Available in Hive 0.13.0 and later)

array_type
  : ARRAY < data_type >

map_type
  : MAP < primitive_type, data_type >

struct_type
  : STRUCT < col_name : data_type [COMMENT col_comment], ...>

union_type
   : UNIONTYPE < data_type, data_type, ... >  -- (Note: Available in Hive 0.7.0 and later)

row_format
  : DELIMITED [FIELDS TERMINATED BY char [ESCAPED BY char]] [COLLECTION ITEMS TERMINATED BY char]
        [MAP KEYS TERMINATED BY char] [LINES TERMINATED BY char]
        [NULL DEFINED AS char]   -- (Note: Available in Hive 0.13 and later)
  | SERDE serde_name [WITH SERDEPROPERTIES (property_name=property_value, property_name=property_value, ...)]

file_format:
  : SEQUENCEFILE
  | TEXTFILE    -- (Default, depending on hive.default.fileformat configuration)
  | RCFILE      -- (Note: Available in Hive 0.6.0 and later)
  | ORC         -- (Note: Available in Hive 0.11.0 and later)
  | PARQUET     -- (Note: Available in Hive 0.13.0 and later)
  | AVRO        -- (Note: Available in Hive 0.14.0 and later)
  | JSONFILE    -- (Note: Available in Hive 4.0.0 and later)
  | INPUTFORMAT input_format_classname OUTPUTFORMAT output_format_classname

column_constraint_specification:
  : [ PRIMARY KEY|UNIQUE|NOT NULL|DEFAULT [default_value]|CHECK  [check_expression] ENABLE|DISABLE NOVALIDATE RELY/NORELY ]

default_value:
  : [ LITERAL|CURRENT_USER()|CURRENT_DATE()|CURRENT_TIMESTAMP()|NULL ]

constraint_specification:
  : [, PRIMARY KEY (col_name, ...) DISABLE NOVALIDATE RELY/NORELY ]
    [, PRIMARY KEY (col_name, ...) DISABLE NOVALIDATE RELY/NORELY ]
    [, CONSTRAINT constraint_name FOREIGN KEY (col_name, ...) REFERENCES table_name(col_name, ...) DISABLE NOVALIDATE
    [, CONSTRAINT constraint_name UNIQUE (col_name, ...) DISABLE NOVALIDATE RELY/NORELY ]
    [, CONSTRAINT constraint_name CHECK [check_expression] ENABLE|DISABLE NOVALIDATE RELY/NORELY ]





set hive.support.quoted.identifiers=None;                                          -- 设置后可以在自动中使用正则表达式，选定字段（字段反选）。默认为 column
select `(id)?+.+` from test_map;                                                   -- 过滤掉 id 字段的其他所有字段

set hive.groupby.orderby.position.alias=true;                                      -- 设置 Hive 可以使用 group by 1,2,3
set hive.resultset.use.unique.column.names=false;                                  -- 设置 Hive 查询结果不显示库名
set hive.cli.print.current.db=true;                                                -- 设置 Hive 显示库名称 hive (default)>
set hive.cli.print.header=true;                                                    -- 设置 Hive 查表记录时，展示字段名称
set hive.auto.convert.join=false;                                                  -- 设置 关闭自动 MapJoin
set hive.variable.substitute.depth=200;                                            -- 设置 替换变量的长度（默认：40）

set spark.sql.shuffle.partitions=2000;                                             -- 设置 shuffle 的并发度（用于sparkSQL）
set spark.default.parallelism=2000;                                                -- 设置 shuffle 的并发度（用于sparkRDD）

set mapred.job.name=my_job_name;                                                   -- 设置 Hive 任务名称
set spark.app.name=my_job_name;                                                    -- 设置 Spark 任务名称

set spark.sql.autoBroadcastJoinThreshold=1073741824;                               -- 设置广播变量的大小（b）（默认10M）设置1G
set spark.locality.wait=0s;
set hive.groupby.skewindata=true;

-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


-- Hive 性能调优
set hive.optimize.reducededuplication.min.reducer=4;
set hive.optimize.reducededuplication=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=false;
set hive.merge.smallfiles.avgsize=16000000;
set hive.merge.size.per.task=256000000;
set hive.merge.sparkfiles=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=20M; -- (might need to increase for Spark, 200M)
set hive.optimize.bucketmapjoin.sortedmerge=false;
set hive.map.aggr.hash.percentmemory=0.5;
set hive.map.aggr=true;
set hive.optimize.sort.dynamic.partition=false;
set hive.stats.autogather=true;
set hive.stats.fetch.column.stats=true;
set hive.compute.query.using.stats=true;
set hive.limit.pushdown.memory.usage=0.4 (MR and Spark);
set hive.optimize.index.filter=true;
set hive.exec.reducers.bytes.per.reducer=67108864;
set hive.smbjoin.cache.rows=10000;
set hive.fetch.task.conversion=more;
set hive.fetch.task.conversion.threshold=1073741824;
set hive.optimize.ppd=true;



set hive.execution.engine=mr;                                                      -- 设置 Hive 执行引擎为 MapReduce
set hive.execution.engine=spark;                                                   -- 设置 Hive 执行引擎为 Spark
set mapreduce.job.queuename=root.default;                                          -- 设置 MapReduce 的 Yarn 对列
set yarn.scheduler.maximum-allocation-mb=16g;                                      -- 设置 ResourceManager 容器内存
set yarn.nodemanager.resource.memory-mb=16g;                                       -- 设置 NodeManager 容器内存
set hive.exec.parallel=true;                                                       -- 设置 多进程并行
set hive.exec.parallel.thread.number=16;                                           -- 设置 多进程数，默认8
set hive.support.quoted.identifiers=None;                                          -- 设置 可以使用正则表达式查找字段
set hive.mapjoin.optimized.hashtable=false;                                        -- 设置 禁用自动 MapJoin
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;                      -- 设置 MapJoin 时的缓存占比
set spark.driver.memory=4g;                                                        -- 设置 Spark Driver 的内存
set spark.driver.memoryOverhead=4g;                                                -- 设置 Spark Driver 的堆外内存
set spark.executor.memory=4g;                                                      -- 设置 Spark Executor 的内存
set spark.executor.memoryOverhead=4g;                                              -- 设置 Spark Executor 的堆外内存
set spark.executor.heartbeatInterval=60s;                                          -- 设置 Spark Executor 通信超时时间
set spark.executor.instances=50;                                                   -- 设置 Executor 进程数
set spark.executor.cores=3;                                                        -- 设置 每个 Executor 的核数
set spark.shuffle.memoryFraction=0.6;                                              -- 设置 Shuffle 操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;                                       -- 设置 抓取数据的最大值
set spark.default.parallelism=1000;                                                -- 设置 Task 数量
set hive.spark.client.future.timeout=360;                                          -- 设置 Spark Client 超时时间
set hive.mapred.mode=nostrict;                                                     -- 设置 非严格模式
set hive.exec.dynamic.partition=true;                                              -- 设置 动态分区
set hive.exec.dynamic.partition.mode=nonstrict;                                    -- 设置 动态分区为非严格模式
set hive.exec.max.dynamic.partitions.pernode=100;                                  -- 设置 每个执行 MR 的节点上，最大可以创建多少个动态分区
set hive.exec.max.dynamic.partitions=1000;                                         -- 设置 所有执行 MR 的节点上，最大可以创建多少个动态分区
set hive.exec.max.created.files=100000;                                            -- 设置 整个 MR Job 中，最大可以创建多少个 HDFS 文件
set hive.error.on.empty.partition=false;                                           -- 设置 当有空分区生成时，是否抛出异常

-- MR 优化
set hive.exec.compress.intermediate=true;                                          -- 设置 MR中间数据可以进行压缩，默认是 false
set hive.intermediate.compression.codec=org.apache.hadoop.io.compress.snappycodec; -- 设置 MR中间数据压缩算法
set hive.exec.compress.output=true;                                                -- 设置 MR输出数据可以进行压缩，默认是 false
set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.snappycodec; -- 设置 MR输出数据压缩算法，Hadoop 的配置

-- 减少 Map 数，小文件较多时
set mapred.max.split.size=‪268435456‬;                                               -- 设置 每个 map 处理的最大的文件大小，单位为‪ 268435456‬B=256M
set mapred.min.split.size.per.node=100000000;                                      -- 设置 每个节点中可以处理的最小的文件大小
set mapred.min.split.size.per.rack=100000000;                                      -- 设置 每个机架中可以处理的最小的文件大小
set hive.exec.reducers.bytes.per.reducer=1073741824;                               -- 设置 每个 reduce 处理的数据量，默认1GB
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;         -- 设置 Hive 的输入进行预聚合

-- 增加 Map 数，在文件中的数据量大的时候，可以拆分成 Map 执行
set mapred.reduce.tasks=10;                                                        -- 设置 reduce 的数量
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=6144;
set mapreduce.reduce.memory.mb=6144;

-- 设置 Reduce 数
set hive.exec.reducers.max=1099;                                                   -- 设置 每个任务最大的 reduce 数，默认为 1099）
-- 计算reducer数的公式很简单 N=min(hive.exec.reducers.max，总输入数据量/mapred.max.split.size)






-- 测试数值排序
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


-- 窗口函数测试数据
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

-- OVER 从句
-- /*
--   OVER 与标准聚合函数 COUNT, SUM, MIN, MAX, AVG
--   使用 PARTITION BY 语句进行 OVER ，该语句可以对任何原始数据的一个或多个列进行分区。
--   使用 PARTITION BY 与 ORDER BY 语句进行 OVER ，该语句可以对任何原始数据的一个或多个列进行分区排序。
--   使用窗口规范，窗口规范支持以下格式： -- num 不可以为 0 ，是正整数
--   (ROW | RANGE) BETWEEN (UNBOUNDED | [num]) PRECEDING AND ([num] PRECEDING | CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
--   (ROW | RANGE) BETWEEN CURRENT ROW                   AND (CURRENT ROW | (UNBOUNDED | [num]) FOLLOWING)
--   (ROW | RANGE) BETWEEN [num] PRECEDING               AND (UNBOUNDED | [num]) FOLLOWING
--   当 ORDER BY 后面缺少窗口从句条件，窗口规范默认是
--   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--   当 ORDER BY 和窗口从句都缺失，窗口规范默认是：
--   ROWS  BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
--  */

-- 测试over从句
SELECT user_id,user_type,price,sales,row_number() over(partition by user_type order by sales,user_id) as row_num,
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales) AS pv1, -- 默认为从起点到当前行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv21, --从起点到当前行，结果同pv1
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv2,
  SUM(price) OVER(PARTITION BY user_type) AS pv3,               --分组内所有行
  SUM(price) OVER(PARTITION BY user_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pv31,
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS pv4,   --当前行+往前3行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS pv5,    --当前行+往前3行+往后1行
  SUM(price) OVER(PARTITION BY user_type ORDER BY sales ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS pv6   ---当前行+往后所有行
from base order by user_type,sales;


-- 窗口函数
-- 行比较分析函数 lead 和 lag 无 window (窗口)子句。
-- /*
--   LEAD(col [,n,DEFAULT]) 用于获取窗口内往下第n行的值。
--     列名，往下第 n 行(可选，默认为 1 )，默认值(可选，当往下第 n 行为 NULL 时，取默认值，默认为 NULL )
--  */
select
  user_id,
  device_id,
  sales,
  row_number()            over(order by sales) as row_num,
  lead(device_id)         over(order by sales) as after_one_line,
  lead(device_id,2)       over(order by sales) as after_two_line,
  lead(device_id,2,'abc') over(order by sales) as after_two_line_default
from base order by sales;

-- /*
--   LAG(col [,n,DEFAULT])  用于获取窗口内往上第n行的值。
--     列名，往上第 n 行(可选，默认为 1 )，默认值(可选，当往上第 n 行为 NULL 时，取默认值，默认为 NULL )
--  */
select
  user_id,
  device_id,
  sales,
  row_number()           over(order by sales) as row_num,
  lag(device_id)         over(order by sales) as before_one_line,
  lag(device_id,2)       over(order by sales) as before_two_line,
  lag(device_id,2,'abc') over(order by sales) as before_two_line_default
from base order by sales;


-- /*
--   FIRST_VALUE 取出分组内排序后，截止到当前行，第一个值。
--               window 子句为 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--  */
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

-- /*
--   LAST_VALUE  取出分组内排序后，截止到当前行，最后一个值。
--               window 子句为 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--  */
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
-- /*
--   ROW_NUMBER()    从1开始，按照顺序，生成分组内记录的序列
--
--   RANK()          生成数据项在分组中的排名，排名相等会在名次中留下空位
--
--   DENSE_RANK()    生成数据项在分组中的排名，排名相等会在名次中不会留下空位
--
--   NTILE(n)  将分组数据按照顺序切分成n片，返回当前切片值。如果切片不均匀，默认增加第一个切片的分布。
--             NTILE不支持ROWS BETWEEN
--  */
select
  user_type,sales,
  NTILE(2) OVER(PARTITION BY user_type ORDER BY sales) AS nt2,    -- 分组内将数据分成 2 片
  NTILE(3) OVER(PARTITION BY user_type ORDER BY sales) AS nt3,    -- 分组内将数据分成 3 片
  NTILE(4) OVER(PARTITION BY user_type ORDER BY sales) AS nt4,    -- 分组内将数据分成 4 片
  NTILE(4) OVER(ORDER BY sales)                        AS all_nt4 -- 将所有数据分成 4 片
from base order by user_type,sales;


-- 注意： 序列函数不支持WINDOW子句
-- CUME_DIST()     小于等于当前值的行数除以分组内总行数 -- 注意的点 小于等于当前值的行数
select
  user_id,user_type,sales,
  --没有partition,所有数据均为1组
  CUME_DIST() OVER(ORDER BY sales) AS cd1,
  --按照user_type进行分组
  CUME_DIST() OVER(PARTITION BY user_type ORDER BY sales) AS cd2
from base order by user_type,sales;

-- PERCENT_RANK()  分组内当前行的 RANK() 值-1/分组内总行数-1
select
  user_type,sales,
  sum(1)         over(partition by user_type)                as s,    -- 分组内总行数
  rank()         over(order by sales)                        as ar,   -- 全局 rank 值
  percent_rank() over(order by sales)                        as apr,  -- 全局 percent_rank 值
  rank()         over(partition by user_type order by sales) as gr,   -- 分组 rank 值
  percent_rank() over(partition by user_type order by sales) as gprg  -- 分组 percent_rank 值
from base order by user_type,sales;


```

#### 4.2.1.2 面试案例
```sql
-- 面试案例
-- /*
--   目前有2张表
--     交易明细表，记录每笔交易信息
--     产品映射表，记录产品码与具体业务场景的映射关系
--   其具体结构如下，请根据这2张基础表，写出对应的查询逻辑。
--   trade_detail 表：（1亿条数据）:
--   user_id,
--   trade_no,
--   product_code,
--   trade_time
--
--   dim_product_scene 表：(100条数据):
--   product_code,
--   biz_scene
--  */


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


-- （1）取每个用户在各个biz_scene下的首次支付时间、最后一次支付时间

SELECT biz_scene,user_id,
  first_value(trade_time) over(partition by biz_scene,user_id order by trade_time)      as first_time,
  first_value(trade_time) over(partition by biz_scene,user_id order by trade_time desc) as last_time
from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
order by biz_scene,user_id;

-- （2）取每个用户在各个biz_scene下的历史第二笔支付时间

SELECT biz_scene,user_id,trade_time
from (
  SELECT biz_scene,user_id,trade_time,row_number() over(partition by biz_scene,user_id order by trade_time) as od
  from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
) as tmp
where od = 2;

-- （3）计算每个用户的平均支付间隔（即：连续交易的时间间隔，取平均）

SELECT user_id,trade_time,
  LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time) as lead_time,
  datediff(to_date(LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time)),to_date(trade_time)) as date_diff,
  sum(datediff(to_date(LEAD(trade_time,1,trade_time) over(partition by user_id order by trade_time)),to_date(trade_time)))/count(1) over(partition by user_id) as avg_time
from dim_product_scene join trade_detail on dim_product_scene.product_code = trade_detail.product_code
order by user_id
;

-- （4）取每个product_code下的总交易用户数（假设超过9000W条数据都是同一个product_code（product_a），需要考虑执行效率。
```


#### 4.2.1.3 Hive 小测试案例
```sql
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
select get_json_object(json,'$.a.aa') as aa from tmp_test_hivemap;


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



### 4.2.2 Hive SQL 语句
```sql
-- Hive 函数操作
hdfs dfs -put ./HiveUDF-1.0.jar /user/hive/auxlib

ADD JAR hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar;
ADD JAR hdfs://node233:8020/user/hive/auxlib/hive-jdbc-handler-1.2.1.jar;
ADD JAR hdfs://node233:8020/user/hive/auxlib/mysql-connector-java.jar;
ADD JAR hdfs://node233:8020/user/hive/auxlib/hive-jdbc.jar;

DROP FUNCTION IF EXISTS encrypt_aes;
DROP FUNCTION IF EXISTS decrypt_aes;
DROP FUNCTION IF EXISTS json_array_to_array;
DROP FUNCTION IF EXISTS map_from_str;
DROP FUNCTION IF EXISTS json_map;
DROP FUNCTION IF EXISTS datefmt;
DROP FUNCTION IF EXISTS age_birth;
DROP FUNCTION IF EXISTS age_idno;
DROP FUNCTION IF EXISTS sex_idno;
DROP FUNCTION IF EXISTS is_empty;
DROP FUNCTION IF EXISTS sha256;
DROP FUNCTION IF EXISTS date_max;
DROP FUNCTION IF EXISTS date_min;

CREATE FUNCTION encrypt_aes         AS 'com.weshare.udf.AesEncrypt'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION decrypt_aes         AS 'com.weshare.udf.AesDecrypt'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION json_array_to_array AS 'com.weshare.udf.AnalysisJsonArray'              USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION map_from_str        AS 'com.weshare.udf.AnalysisStringToJson'           USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION json_map            AS 'com.weshare.udf.AnalysisStringToJsonGenericUDF' USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION datefmt             AS 'com.weshare.udf.DateFormat'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_birth           AS 'com.weshare.udf.GetAgeOnBirthday'               USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION age_idno            AS 'com.weshare.udf.GetAgeOnIdNo'                   USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sex_idno            AS 'com.weshare.udf.GetSexOnIdNo'                   USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
-- CREATE FUNCTION is_empty            AS 'com.weshare.udf.IsEmpty'                        USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION is_empty            AS 'com.weshare.udf.IsEmptyGenericUDF'              USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION sha256              AS 'com.weshare.udf.Sha256Salt'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_max            AS 'com.weshare.udf.GetDateMax'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';
CREATE FUNCTION date_min            AS 'com.weshare.udf.GetDateMin'                     USING JAR 'hdfs://node233:8020/user/hive/auxlib/HiveUDF-1.0-shaded.jar';

reload function; -- 多个 HiveServer 之间，需要同步元数据信息

SHOW FUNCTIONS LIKE 'default*';
DESC FUNCTION EXTENDED is_empty;

SHOW FUNCTIONS LIKE '*map*';
DESC FUNCTION EXTENDED day;
```



## 4.3 Impala
### 4.3.1 Impala 学习
```sql
```

### 4.3.2 Impala SQL 语句
```sql
-- 时间设置
set use_local_tz_for_unix_timestamp_conversions=true;
-- impala 同步 hive [表] 元数据
invalidate metadata;
invalidate metadata [table];
-- impala 刷新数据库
refresh dwb.dwb_credit_apply;
refresh [table] [partition [partition]];

-- impala 函数操作
show functions in _impala_builtins like '*nvl*';

create function encrypt_aes(string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0-shaded.jar' symbol='com.weshare.udf.Aes_Encrypt';
create function encrypt_aes(string, string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0-shaded.jar' symbol='com.weshare.udf.Aes_Decrypt';

create function decrypt_aes(string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0-shaded.jar' symbol='com.weshare.udf.Aes_Encrypt';
create function decrypt_aes(string, string) returns string location '/opt/cloudera/hive/auxlib/HiveUDF-1.0-shaded.jar' symbol='com.weshare.udf.Aes_Decrypt';
```



# 4、Excel 操作
```vbnet
# excel的十字光标
Private Sub Workbook_SheetSelectionChange(ByVal Sh As Object, ByVal Target As Range)
    Cells.Interior.ColorIndex = xlNone
    Rows(Target.Row).Interior.Color = RGB(0,255,255)
    Columns(Target.Column).Interior.Color = RGB(0,255,255)
End Sub
# ODBC连接字符串
Driver={MySQL ODBC 8.0 Unicode Driver};server:10.10.18.48;database=dm_cf;
```

# 5、Markdown 操作
| 操作名称 |                    快捷键                     |                                              代码                                             |                                             结果                                            |
|----------|-----------------------------------------------|-----------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| 插入图片 | Shift + Win + K                               | `<img src="https://img-blog.csdnimg.cn/20200617115400965.jpg" alt="图片" style="zoom:10%;"/>` | <img src="https://img-blog.csdnimg.cn/20200617115400965.jpg" alt="图片" style="zoom:20%;"/> |
| 超链接   | Ctrl + Alt + V                                | `[百度](http://www.baidu.com)`                                                                | [百度](https://img-blog.csdnimg.cn/20200617115400965.jpg)                                   |
| 引用     | Ctrl + Alt + R 点击快捷键后，直接输入文字即可 | `[引用][引用]  [引用]:http://www.baidu.com`                                                   | [引用][引用]  [引用]:http://www.baidu.com                                                   |
| 插入注释 | Alt + Shift + 6                               | `注释引用[^1]  [^1]: http://www.baidu.com`                                                    | 注释引用[^1]  [^1]: http://www.baidu.com                                                    |
| 加粗文本 |                                               | `**加粗文本** __加粗文本__`                                                                   | **加粗文本** __加粗文本__                                                                   |
| 标记文本 |                                               | `==标记文本==`                                                                                | ==标记文本==                                                                                |
| 删除文本 |                                               | `~~删除文本~~`                                                                                | ~~删除文本~~                                                                                |
| 引用文本 |                                               | `> 引用文本`                                                                                  | > 引用文本                                                                                  |
| 下标     |                                               | `H<sub>2</sub>O is是液体。`                                                                   | H<sub>2</sub>O is是液体。                                                                   |
| 幂运算   |                                               | `2<sup>10</sup> 运算结果是 1024`                                                              | 2<sup>10</sup> 运算结果是 1024                                                              |



# 6、Sublime 操作
## 6.1 Sublime 远程同步文件操作到 Linux
在想要添加同步的文件中右键点击“SFTP/FTP”，选择 Map to Remote 进行编辑

<img src="https://img-blog.csdnimg.cn/20200526103111136.png" alt="图片" style="zoom:70%;"/>
```javascript
{
    // The tab key will cycle through the settings when first created
    // Visit http://wbond.net/sublime_packages/sftp/settings for help

    // sftp, ftp or ftps
    "type": "sftp",

    "save_before_upload": true,
    "upload_on_save": true,             // 由 false 修改为 true，作用是在保存本地时，同步保存远端
    "sync_down_on_open": false,
    "sync_skip_deletes": false,
    "sync_same_age": true,
    "confirm_downloads": false,
    "confirm_sync": true,
    "confirm_overwrite_newer": false,

    "host": "localhost",                // 修改为想要同步的 Linux 的 ip 或 host。由 example.com 修改为 ip
    "user": "username",                 // 修改为自己的 Linux 用户名
    "password": "password",             // 将这一行的注释打开，并填写自己的密码
    //"port": "22",                     // 有必要的情况下打开此项

    "remote_path": "/example/path/",    // 修改为远程的目录
    "ignore_regexes": [
        "\\.sublime-(project|workspace)", "sftp-config(-alt\\d?)?\\.json",
        "sftp-settings\\.json", "/venv/", "\\.svn/", "\\.hg/", "\\.git/",
        "\\.bzr", "_darcs", "CVS", "\\.DS_Store", "Thumbs\\.db", "desktop\\.ini"
    ],
    //"file_permissions": "664",
    //"dir_permissions": "775",

    //"extra_list_connections": 0,

    "connect_timeout": 30,
    //"keepalive": 120,
    //"ftp_passive_mode": true,
    //"ftp_obey_passive_host": false,
    //"ssh_key_file": "~/.ssh/id_rsa",
    //"sftp_flags": ["-F", "/path/to/ssh_config"],

    //"preserve_modification_times": false,
    //"remote_time_offset_in_hours": 0,
    //"remote_encoding": "utf-8",
    //"remote_locale": "C",
    //"allow_config_upload": false,
}
```

## 6.2 Sublime 使用 OmniMarkupPreviewer 时的问题
```javascript
// 安装 OmniMarkupPreviewer 后，浏览器打开报 404，修改配置文件
"renderer_options-MarkdownRenderer": {
  "extensions": [
    "tables",
    "toc",
    "fenced_code",
    "codehilite"
  ]
}
// 安装 Pretty Json 后，浏览器查看失效
"renderer_options-MarkdownRenderer": {
  "extensions": [
    "markdown.extensions.tables",
    "markdown.extensions.toc",
    "markdown.extensions.fenced_code",
    "markdown.extensions.codehilite"
  ]
}
```
## 6.3 Sublime 配置
|                名称               |                             配置                             |                      解释                      |                                       示例                                      |            备注           |
|-----------------------------------|--------------------------------------------------------------|------------------------------------------------|---------------------------------------------------------------------------------|---------------------------|
| auto_find_in_selection            | true                                                         | 开启选中范围内搜索                             | "auto_find_in_selection": true,                                                 |                           |
| bold_folder_labels                | true                                                         | 侧边栏文件夹显示加粗，区别于文件               | "bold_folder_labels": true,                                                     |                           |
| color_scheme                      | Packages/Color Scheme - Default/Monokai.sublime-color-scheme | 颜色格式设置                                   | "color_scheme": "Packages/Color Scheme - Default/Monokai.sublime-color-scheme", |                           |
| default_encoding                  | UTF-8                                                        | 设置默认编码格式为 UTF-8                       | "default_encoding": "UTF-8",                                                    |                           |
| default_line_ending               | unix                                                         | 使用 unix 风格的换行符                         | "default_line_ending": "unix",                                                  |                           |
| draw_minimap_border               | true                                                         | 用于右侧代码预览时给所在区域加上边框，方便识别 | "draw_minimap_border": true,                                                    |                           |
| draw_white_space                  | all                                                          | 显示Tab、空格                                  | "draw_white_space": "all",                                                      |                           |
| ensure_newline_at_eof_on_save     | true                                                         | 文件末尾自动保留一个空行                       | "ensure_newline_at_eof_on_save": true,                                          |                           |
| fade_fold_buttons                 | false                                                        | 默认显示行号右侧的代码段闭合展开三角号         | "fade_fold_buttons": false,                                                     |                           |
| font_face                         | Microsoft YaHei Mono                                         | 设置字体                                       | "font_face": "Microsoft YaHei Mono",                                            |                           |
| font_options                      | gdi                                                          | 设置字体适配                                   | "font_options": ["gdi",],                                                       | gdi仅适用于Windows        |
| font_size                         | 15                                                           | 设置字体大小为15号                             | "font_size": 15,                                                                |                           |
| highlight_line                    | true                                                         | 高亮编辑的那一行                               | "highlight_line": true,                                                         |                           |
| highlight_modified_tabs           | true                                                         | 高亮未保存文件                                 | "highlight_modified_tabs": true,                                                |                           |
| ignored_packages                  | Vintage                                                      | 过滤Package Control包                          | "ignored_packages": ["Vintage"],                                                | 去除后可以使用Vim编辑模式 |
| line_numbers                      | true                                                         | 设置是否显示行号                               | "line_numbers": true,                                                           |                           |
| rulers                            | 126                                                          | 在长度为126时，显示分割线                      | "rulers":[126],                                                                 |                           |
| save_on_focus_lost                | true                                                         | 窗口失焦立即保存文件                           | "save_on_focus_lost": true,                                                     |                           |
| show_encoding                     | true                                                         | 显示文件编码格式                               | "show_encoding": true,                                                          |                           |
| show_full_path                    | true                                                         | 显示全路径                                     | "show_full_path": true,                                                         |                           |
| show_line_endings                 | true                                                         | 显示换行符格式                                 | "show_line_endings": true,                                                      |                           |
| tab_size                          | 2                                                            | tab的长度                                      | "tab_size": 2,                                                                  |                           |
| theme                             | Default.sublime-theme                                        | 主题设置                                       | "theme": "Default.sublime-theme",                                               |                           |
| translate_tabs_to_spaces          | true                                                         | true为空格替换TAB键，false则是TAB键            | "translate_tabs_to_spaces": true,                                               |                           |
| trim_trailing_white_space_on_save | true                                                         | 自动移除行尾多余空格                           | "trim_trailing_white_space_on_save": true,                                      |                           |
| update_check                      | false                                                        | 关闭自动检测升级                               | "update_check": false,                                                          |                           |
| word_wrap                         | true                                                         | 设置自动换行                                   | "word_wrap": true,                                                              |                           |
| wrap_width                        | 0                                                            | 设置单行的宽度（0为不设置）                    | "wrap_width": 0,                                                                |                           |
