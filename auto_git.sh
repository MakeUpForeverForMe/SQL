#!/bin/bash -e

prt(){ echo -e "$(date +'%F %T') $(printf "%${2:-10}s" | sed "s/ /$1/g")\n\n"; }

succ_erro(){ aa=$? && ( [[ $aa == 0 ]] && prt '成功' || prt '错误' ) &>> $log; }


get_file(){
  from_root_dir=/d/Users/ximing.wei/Desktop/技术中心/数仓表结构
  copy_root_dir=/d/Users/ximing.wei/Desktop/code/Project
  for file in ${1:-$from_root_dir}/*; do
    [[ "$file" =~ git ]] && continue
    tag_file="$copy_root_dir/数仓表结构/$(echo "${file//"${from_root_dir}/"/}")"
    [[ -d "$file" ]] && {
      [[ ! -d "$tag_file" ]] && mkdir "$tag_file"
      get_file "$file"
    } || {
      [[ -f "$file" ]] && {
        printf '%-145s\t%s\n' "$file" "$tag_file"
        rm "$tag_file"
        link "$file" "$tag_file"
      }
    }
  done
}


dir1=/d/Users/ximing.wei/Desktop/code
dir2=/d/Users/ximing.wei/Desktop/技术中心


dirs=$dir1,$dir1/Project,$dir1/starsource,$dir2/数仓表结构
# dirs=$dir1/Project
log=$dir1/auto_git.log

prt '-' '50' &>> $log

for dir in ${dirs//,/ }; do
  cd $dir
  pwd &>> $log
  git pull &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  [[ $dir =~ $dir1/Project ]] && get_file &>> $log
  # exit
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
