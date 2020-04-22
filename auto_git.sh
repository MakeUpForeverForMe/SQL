#!/bin/bash -e

prt(){ echo -e "$(date +'%F %T') $(printf "%${2:-10}s" | sed "s/ /$1/g")"; }

succ_erro(){ aa=$? && ( [[ $aa == 0 ]] && prt '成功' || prt '错误' ) &>> $log; }


dir1=/d/Users/ximing.wei/Desktop/code
dir2=/d/Users/ximing.wei/Desktop/技术中心


dirs=$dir1,$dir1/Project,$dir2/数仓表结构
log=$dir1/auto_git.log
prt '-' '50' &>> $log

for dir in ${dirs//,/ }; do
  cd $dir
  pwd &>> $log

  echo 'git add -u' &>> $log
  git add -u >&2 &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  msg="$(date +%F) 系统自动提交"
  echo "git commit -m '$msg'" &>> $log
  git commit -m "$msg" >&2 &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  echo 'git push' &>> $log
  git push >&2 &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  echo
done

printf '\n\n' &>> $log
