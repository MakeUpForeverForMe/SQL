@echo off & mode con cols=44 lines=24 & title=�ػ����� & color 0a

rem �����ӳٵ�������䲢�ö�̾�š�!!�������� setlocal enabledelayedexpansion
rem �ı䴰�ڴ�С			mode con cols=44 lines=17
rem ���ô��ڵı���		title=���ô��ڱ���
rem �ı����弰������ɫ	color [fb] (f:������ɫ b:���ڱ�����ɫ)
rem 0 = ��ɫ		8 = ��ɫ
rem 1 = ��ɫ		9 = ����ɫ
rem 2 = ��ɫ		A = ����ɫ
rem 3 = ����ɫ	B = ��ǳ��ɫ
rem 4 = ��ɫ		C = ����ɫ
rem 5 = ��ɫ		D = ����ɫ
rem 6 = ��ɫ		E = ����ɫ
rem 7 = ��ɫ		F = ����ɫ

:param
echo ++++++++++++++++++++++++++++++++++++++++++++
echo +                  ������                  +
echo +                                          +
echo +                e:�˳�                    +
echo +                                          +
echo +                0:�����ػ�                +
echo +                                          +
echo +                1:��������                +
echo +                                          +
echo +                2:�����ػ�                +
echo +                                          +
echo +                3:��ʱ�ػ�                +
echo +                                          +
echo +                4:����ʱ�ػ�              +
echo +                                          +
echo +                5:ȡ���ػ�                +
echo +                                          +
echo +                6:��ѯ�ػ�����            +
echo +                                          +
echo +                7:ɾ���ػ�����            +
echo ++++++++++++++++++++++++++++++++++++++++++++

set /p param="������ѡ�"

if %param%==0 (goto halt)
if %param%==1 (goto reboot)
if %param%==2 (goto shutdown)
if %param%==3 (goto timed)
if %param%==4 (goto countdown)
if %param%==5 (goto cancel)
if %param%==6 (goto list)
if %param%==7 (goto delete)
if %param%==e (goto exit) else echo ������� && goto param

rem �˳�
:exit
exit

rem �����ػ�
:halt
start "" shutdown /s /c �رռ���� /t 0
goto param

rem ��������
:reboot
start "" shutdown /r /c ��������� /t 0
goto param

rem �»��ػ�
:shutdown
start "" "C:\Windows\System32\SlideToShutDown.exe"
goto param

rem ��ʱ�ػ�
:timed
set /p minute="������ػ�ʱ��(�磺22:00:00[e�˳�])��"
if %minute%==e (echo ����ѡ� & goto param)
schtasks /delete /f /tn "shutdown"
schtasks /create /f /tn "shutdown" /sc once /st %minute% /tr "shutdown /s"
goto param

rem ����ʱ�ػ�
:countdown
set /p minute="������ػ�����ʱ����(e�˳�)��"
if %minute%==e (echo ����ѡ� & goto param) else for /f "delims=1234567890" %%a in ("@%minute%@") do if %%a==@ (shutdown /s /t %minute%) else echo ���Ǵ����� && goto countdown
goto param

rem ȡ���ػ�
:cancel
shutdown /a
goto param

rem ��ѯ�ػ�����
:list
schtasks /Query /tn "shutdown"
goto param

rem ɾ���ػ�����
:delete
rem set /p task="�������������ƣ�"
rem schtasks /delete /f /tn %task%
schtasks /delete /f /tn "shutdown"
goto param
