@echo off
chcp 65001 > nul

setlocal enabledelayedexpansion

set programs[1-1]=go run ./group1/kv1_1.go
set programs[1-2]=go run ./group1/kv1_2.go
set programs[1-3]=go run ./group1/kv1_3.go
set programs[2-1]=go run ./group2/kv2_1.go
set programs[2-2]=go run ./group2/kv2_2.go
set programs[2-3]=go run ./group2/kv2_3.go

set ports[1-1]=7001
set ports[1-2]=7002
set ports[1-3]=7003
set ports[2-1]=7004
set ports[2-2]=7005
set ports[2-3]=7006

set startedServers=

:main
set /p input=$:

for /f "tokens=1-3 delims= " %%a in ("%input%") do (
    set command=%%a
    set group=%%b
    set server=%%c
)


if /i "%command%"=="start" (
    set "program=!programs[%group%-%server%]!"
	echo start server:!server! in group:!group!
    if defined program (
        start "" /B cmd /C "!program!"
        ping 127.0.0.1 -n 4 > nul
    ) else (
        echo server:%server% in group:%group% not exists
    )
) else if /i "%command%"=="stop" (
    set "port=!ports[%group%-%server%]!"
	echo stop server:!server! in group:!group!
	if defined port (
		for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":!port!"') do (
			set "pid=%%a"
			set "running=1"
		)
		if defined pid (
			taskkill /F /PID !pid! > nul 2>&1
			if !errorlevel! equ 0 (
				echo Process with PID !pid! on port !port! has been terminated.
				ping 127.0.0.1 -n 1 > nul
			) else (
				echo Failed to terminate process with PID !pid! on port !port!.
			)
		)
		set "pid="
		if not defined running (
			echo server:%server% in group:%group% is not running
		)
		set "running="
	) else (
		echo server:%server% in group:%group% not exists
	)
) else if /i "%command%"=="help" (
	echo 用法: 
    echo     start groupId[1-2] serverId[1-3]   启动指定组上服务器.
    echo     stop  groupId[1-2] serverId[1-3]   停止指定的组和服务器.
    echo     help                     			显示帮助信息.
    goto main
) else if /i "%command%"=="exit" (
    exit /b
) else (
    echo 无效的命令，请使用help获取帮助信息
)

goto main
