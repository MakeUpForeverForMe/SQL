#!/bin/bash -e

n_prt(){ printf "\n\n%$2s\n" | sed "s/ /$1/g"; }
prt_n(){ printf "\n%$2s\n\n" | sed "s/ /$1/g"; }

curr_date=$(date +%F)
dir_home=/d/Users/ximing.wei/Desktop/code

log=$dir_home/auto_git.log

dirs=.,AKKA,starsource,PyETL

n_prt '-' '120' &>> $log

date +'%F %T' &>> $log

for dir in ${dirs//,/ }; do
  cd $dir_home/$dir
  # pwd >&2
  pwd &>> $log

  echo 'git add -u' &>> $log
  git add -u >&2 &>> $log
  [[ $? == 0 ]] && prt_n '成功' '30' &>> $log || { prt_n '错误' '30'; continue; } &>> $log

  echo "git commit -m '$curr_date 系统自动提交'" &>> $log
  git commit -m "$curr_date 系统自动提交" >&2 &>> $log
  [[ $? == 0 ]] && prt_n '成功' '30' &>> $log || { prt_n '错误' '30'; continue; } &>> $log

  echo 'git push' &>> $log
  git push >&2 &>> $log
  [[ $? == 0 ]] && prt_n '成功' '30' &>> $log || { prt_n '错误' '30'; continue; } &>> $log
done


# sleep 5

