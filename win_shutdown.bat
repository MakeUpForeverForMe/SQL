@echo off & mode con cols=44 lines=24 & title=关机设置 & color 0a

rem 变量延迟的启动语句并用对叹号“!!”括起来 setlocal enabledelayedexpansion
rem 改变窗口大小			mode con cols=44 lines=17
rem 设置窗口的标题		title=设置窗口标题
rem 改变字体及背景颜色	color [fb] (f:字体颜色 b:窗口背景颜色)
rem 0 = 黑色		8 = 灰色
rem 1 = 蓝色		9 = 淡蓝色
rem 2 = 绿色		A = 淡绿色
rem 3 = 湖蓝色	B = 淡浅绿色
rem 4 = 红色		C = 淡红色
rem 5 = 紫色		D = 淡紫色
rem 6 = 黄色		E = 淡黄色
rem 7 = 白色		F = 亮白色

:param
echo ++++++++++++++++++++++++++++++++++++++++++++
echo +                  请输入                  +
echo +                                          +
echo +                e:退出                    +
echo +                                          +
echo +                0:立即关机                +
echo +                                          +
echo +                1:重新启动                +
echo +                                          +
echo +                2:滑动关机                +
echo +                                          +
echo +                3:定时关机                +
echo +                                          +
echo +                4:倒计时关机              +
echo +                                          +
echo +                5:取消关机                +
echo +                                          +
echo +                6:查询关机任务            +
echo +                                          +
echo +                7:删除关机任务            +
echo ++++++++++++++++++++++++++++++++++++++++++++

set /p param="请输入选项："

if %param%==0 (goto halt)
if %param%==1 (goto reboot)
if %param%==2 (goto shutdown)
if %param%==3 (goto timed)
if %param%==4 (goto countdown)
if %param%==5 (goto cancel)
if %param%==6 (goto list)
if %param%==7 (goto delete)
if %param%==e (goto exit) else echo 输入错误 && goto param

rem 退出
:exit
exit

rem 立即关机
:halt
start "" shutdown /s /c 关闭计算机 /t 0
goto param

rem 重新启动
:reboot
start "" shutdown /r /c 重启计算机 /t 0
goto param

rem 下滑关机
:shutdown
start "" "C:\Windows\System32\SlideToShutDown.exe"
goto param

rem 定时关机
:timed
set /p minute="请输入关机时间(如：22:00:00[e退出])："
if %minute%==e (echo 返回选项卡 & goto param)
schtasks /delete /f /tn "shutdown"
schtasks /create /f /tn "shutdown" /sc once /st %minute% /tr "shutdown /s"
goto param

rem 倒计时关机
:countdown
set /p minute="请输入关机倒计时秒数(e退出)："
if %minute%==e (echo 返回选项卡 & goto param) else for /f "delims=1234567890" %%a in ("@%minute%@") do if %%a==@ (shutdown /s /t %minute%) else echo 不是纯数字 && goto countdown
goto param

rem 取消关机
:cancel
shutdown /a
goto param

rem 查询关机任务
:list
schtasks /Query /tn "shutdown"
goto param

rem 删除关机任务
:delete
rem set /p task="请输入任务名称："
rem schtasks /delete /f /tn %task%
schtasks /delete /f /tn "shutdown"
goto param
