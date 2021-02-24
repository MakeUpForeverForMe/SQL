#!/bin/bash -e

prt(){ echo -e "$(date +'%F %T') $(printf "%${2:-10}s" | sed "s/ /$1/g")\n\n"; }

succ_erro(){ aa=$? && ( [[ $aa == 0 ]] && prt '成功' || prt '错误' ) &>> $log; }

get_file(){
  files=${1}
  copy_dir=${2}
  [[ $files =~ src$ ]] && {
    file_pom=$files/../pom.xml
    copy_pom=${copy_dir}/../pom.xml
    printf '%-135s\t%-120s\n' ${file_pom} ${copy_pom}
    rm ${copy_pom}
    link ${file_pom} ${copy_pom}
  }
  for file in $files/*; do
    [[ -d $file ]] && get_file ${file} ${copy_dir} || {
      [[ -f $file ]] && {
        if [[ $file =~ src ]]; then
          copy_file=${copy_dir}/$(echo ${file/\/src\// } | awk '{print $2}')
        elif [[ $file =~ data_shell ]]; then
          copy_file=${copy_dir}/$(echo ${file/\/data_shell\// } | awk '{print $2}')
        fi
        printf '%-160s\t%-s\n' ${file} ${copy_file} #&>> $log
        # rm ${copy_file}
        # link ${file} ${copy_file}
      }
    }
  done
}




dir1=/d/Users/ximing.wei/Desktop/code
dir2=/d/Users/ximing.wei/Desktop/技术中心


dirs=$dir1,$dir1/HiveUDF,$dir1/data_shell,$dir1/Project,$dir1/python,$dir1/starsource,$dir2/数仓表结构
# dirs=$dir1/data_shell
log=$dir1/auto_git.log

prt '-' '50' &>> $log

for dir in ${dirs//,/ }; do
  cd $dir
  pwd &>> $log
  git pull &>> $log
  succ_erro && [[ $aa != 0 ]] && continue

  [[ $dir =~ $dir1/HiveUDF ]] && get_file $dir2/数仓表结构/HiveUDF/src $dir1/HiveUDF/src &>> $log
  [[ $dir =~ $dir1/data_shell ]] && get_file $dir2/数仓表结构/data_shell $dir1/data_shell &>> $log
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
