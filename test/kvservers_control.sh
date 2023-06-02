#!/bin/bash

declare -A programs
declare -A serverPids

programs["1-1"]="go run ./group1/kv1_1.go"
programs["1-2"]="go run ./group1/kv1_2.go"
programs["1-3"]="go run ./group1/kv1_3.go"
programs["2-1"]="go run ./group2/kv2_1.go"
programs["2-2"]="go run ./group2/kv2_2.go"
programs["2-3"]="go run ./group2/kv2_3.go"

while true; do
  read -p "$: " command
  IFS=' '
  read -ra inputs <<< "$command"
  case "${inputs[0]}" in
  "start")
    # 拼凑字符串 k
    k="${inputs[1]}-${inputs[2]}"

    # 使用 k 索引 programs 获取要运行的程序位置
    program="${programs[$k]}"

    if [ -n "$program" ]; then
      # 运行程序
      $program & # 后台运行程序，并记录进程pid
      pid=$!
      serverPids["$k"]=$pid # 记录进程pid
      sleep 2
    else
      echo "server:${inputs[2]} for group:${inputs[1]} not exists"
    fi
    ;;
  "stop")
    k="${inputs[1]}-${inputs[2]}"

    pid="${serverPids[$k]}"

    if [ -n "$pid" ] && [ "$pid" -ne 0 ]; then
      # 停止进程
      kill "$pid"
      echo "ok"
    else
      echo "program not exists"
    fi
    unset serverPids["$k"]
    ;;
  "exit")
for pid in "${serverPids[@]}"; do
      if [ -n "$pid" ] && [ "$pid" -ne 0 ]; then
        kill "$pid"
      fi
    done
    exit 0
    ;;
  *)
    echo "command is invalid"
    ;;
  esac
done
