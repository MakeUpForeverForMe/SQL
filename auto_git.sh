#!/bin/bash -e

prt(){ echo -en "\n$(date +'%F %T') $(printf "%${2:-10}s" | sed "s/ /$1/g")\n"; }

succ_erro(){ ( [[ $? == 0 ]] && prt '成功' || { prt '错误'; continue; } ) &>> $log; }

curr_date=$(date +%F)
dir_home=/d/Users/ximing.wei/Desktop/code

log=$dir_home/auto_git.log

dirs=.,Project

dt=$(date +'%F %T')

prt '-' '50' &>> $log


for dir in ${dirs//,/ }; do
  cd $dir_home/$dir
  # pwd >&2
  pwd &>> $log

  echo 'git add -u' &>> $log
  git add -u >&2 &>> $log
  succ_erro

  echo "git commit -m '$curr_date 系统自动提交'" &>> $log
  git commit -m "$curr_date 系统自动提交" >&2 &>> $log
  succ_erro

  echo 'git push' &>> $log
  git push >&2 &>> $log
  succ_erro
done


# sleep 5

