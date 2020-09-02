#!/bin/bash -e

prt(){ echo -e "$(date +'%F %T') $(printf "%${2:-10}s" | sed "s/ /$1/g")\n\n"; }

succ_erro(){ aa=$? && ( [[ $aa == 0 ]] && prt '成功' || prt '错误' ) &>> $log; }

get_file(){
  dir=${1}
  for file in $dir/*; do
    [[ -d $file ]] && get_file $file || {
      [[ -f $file ]] && {
        echo $file /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45} &>> $log
        rm /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45}
        link $file /d/Users/ximing.wei/Desktop/技术中心/数仓表结构/HiveUDF/src/${file:45}
      }
    }
  done
}

dir1=/d/Users/ximing.wei/Desktop/code
dir2=/d/Users/ximing.wei/Desktop/技术中心


dirs=$dir1,$dir1/Project,$dir1/python,$dir1/starsource,$dir2/数仓表结构
log=$dir1/auto_git.log

prt '-' '50' &>> $log

for dir in ${dirs//,/ }; do
  [[ $dir =~ /d/Users/ximing.wei/Desktop/技术中心/数仓表结构 ]] && get_file /d/Users/ximing.wei/Desktop/code/HiveUDF/src &>> $log
  cd $dir
  pwd &>> $log

  echo -e '\n' &>> $log

  echo "git add -u $dir" &>> $log
  git add -u &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  msg="$(date +%F) 系统自动提交"
  echo "git commit -m '$msg'" &>> $log
  git commit -m "$msg" &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  echo 'git push' &>> $log
  git push &>> $log
  succ_erro && [[ $aa != 0 ]] && continue
done

printf '\n\n' &>> $log
