set @year_date_s=20191224
;
set @year_date_e=DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
;
set @year_date_e=@year_date_s
;


-- 所有明细
SELECT
cast(交换时间 AS date) AS 交换时间,
CASE
  WHEN 交换状态 = '流量交换开始' AND 交换量 = '换量正常' THEN '换量正常'
  WHEN 交换状态 != '流量交换开始' AND 交换量 = '换量正常' THEN CONCAT('停止 : ',交换状态)
  WHEN 交换状态 = '流量交换开始' AND 交换量 != '换量正常' THEN CONCAT('停止 : ',交换量)
  WHEN 交换状态 != '流量交换开始' AND 交换量 != '换量正常' THEN CONCAT('停止 : 数量、状态均达阈值',交换状态,'、',交换量)
END AS '交换情况',
广告位App,
广告位名称,
获客计划App,
获客计划名称,
下发数,
展示数,
点击数,
点击率,
广告位编号,
获客计划编号,
广告位展示上限,
广告位交换数量,
获客展示上限,
获客交换数量
FROM (
  SELECT
  advs.report_date                    AS '交换时间',
  app_adv.app_name                    AS '广告位App',
  advs.adv_id                         AS '广告位编号',
  advi.advertise_name                 AS '广告位名称',
  advi.top_limit                      AS '广告位展示上限',
  advi.real_total                     AS '广告位交换数量',
  CASE exin.`status`
    WHEN 1 THEN CONCAT('申请人 ',app_apply.app_name,' 已申请')
    WHEN 2 THEN CONCAT('审核人 ',app_audit.app_name,' 审核中')
    WHEN 3 THEN CONCAT('审核人 ',app_audit.app_name,' 审核不通过')
    WHEN 4 THEN CONCAT('审核人 ',app_audit.app_name,' 审核通过')
    WHEN 5 THEN CONCAT('申请人 ',app_apply.app_name,' 审核中')
    WHEN 6 THEN CONCAT('申请人 ',app_apply.app_name,' 审核不通过')
    WHEN 7 THEN '流量交换开始'
    WHEN 8 THEN CONCAT('审核人 ',app_audit.app_name,' 暂停流量交换')
    WHEN 9 THEN CONCAT('申请人 ',app_apply.app_name,' 暂停流量交换')
    WHEN 10 THEN CONCAT('申请人 ',app_apply.app_name,' 停止流量交换')
    WHEN 11 THEN CONCAT('审核人 ',app_audit.app_name,' 停止流量交换')
    WHEN 12 THEN '系统停止'
    WHEN 13 THEN CONCAT('申请人 ',app_apply.app_name,' 审核通过')
    WHEN 14 THEN '待生效(生效变为7)'
    WHEN 999 THEN '删除'
    ELSE exin.`status` END            AS '交换状态',
  CASE
    WHEN advi.real_total < advi.top_limit AND acqp.sum_count < acqp.sum_budget THEN '换量正常'
    WHEN advi.real_total >= advi.top_limit AND acqp.sum_count < acqp.sum_budget THEN CONCAT('广告位方 ',app_adv.app_name,' 交换量达上限')
    WHEN advi.real_total < advi.top_limit AND acqp.sum_count >= acqp.sum_budget THEN CONCAT('获客计划方 ',app_plan.app_name,' 交换量达上限')
    WHEN advi.real_total >= advi.top_limit AND acqp.sum_count >= acqp.sum_budget THEN '换量双方交换量均达上限'
  END                                             AS '交换量',
  -- IF(advi.real_total < advi.top_limit,'交换量未达上限','交换量已达上限') AS '广告位交换量',
  -- IF(acqp.sum_count < acqp.sum_budget,'交换量未达上限','交换量已达上限') AS '获客计划交换量',
  -- IF(advi.real_total < advi.top_limit,IF(acqp.sum_count < acqp.sum_budget,'换量正常','获客计划交换量达上限'),'广告位交换量达上限') AS '交换量',
  app_plan.app_name                               AS '获客计划App',
  advs.plan_id                                    AS '获客计划编号',
  acqp.`name`                                     AS '获客计划名称',
  acqp.sum_budget                                 AS '获客展示上限',
  acqp.sum_count                                  AS '获客交换数量',
  sum(advs.adv_iss_num)                           AS '下发数',
  sum(advs.adv_show_num)                          AS '展示数',
  sum(advs.adv_cli_num)                           AS '点击数',
  sum(advs.adv_cli_num)/sum(advs.adv_show_num)    AS '点击率'
  FROM ADVERTISING_SPACE AS advs
  LEFT JOIN ADVERTISEMENT_INFO AS advi ON advs.adv_id = advi.advertise_id
  LEFT JOIN ACQUISITION_PLAN AS acqp ON advs.plan_id = acqp.acquisition_id
  LEFT JOIN APP_INFO AS app_adv ON advs.adv_app_id = app_adv.app_id
  LEFT JOIN APP_INFO AS app_plan ON advs.plan_app_id = app_plan.app_id
  LEFT JOIN EXCHANGE_INFO exin
  ON (advs.adv_id = exin.audit_adver_id AND advs.plan_adv_id = exin.apply_adver_id AND advs.plan_id = exin.apply_plan_id)
  OR (advs.adv_id = exin.apply_adver_id AND advs.plan_adv_id = exin.audit_adver_id AND advs.plan_id = exin.audit_plan_id)
  LEFT JOIN APP_INFO AS app_apply ON exin.apply_app_id = app_apply.app_id
  LEFT JOIN APP_INFO AS app_audit ON exin.audit_app_id = app_audit.app_id
  WHERE advi.advertise_name IS NOT NULL AND acqp.`name` IS NOT NULL AND app_adv.app_name IS NOT NULL AND app_plan.app_name IS NOT NULL AND advs.plan_id != 0 AND advs.report_date BETWEEN @year_date_s AND @year_date_e
  GROUP BY advs.report_date,app_adv.app_name,advi.advertise_name,app_plan.app_name,acqp.`name`,advs.adv_iss_num,advs.adv_show_num,advs.adv_cli_num
  ORDER BY advs.report_date DESC
) AS detail
;








SELECT
advs.report_date                    AS '交换时间',
app_adv.app_name                    AS '广告位App',
advi.advertise_name                 AS '广告位名称',
app_plan.app_name                   AS '获客计划App',
acqp.`name`                         AS '获客计划名称',
advs.adv_iss_num                    AS '下发数',
advs.adv_show_num                   AS '展示数',
advs.adv_cli_num                    AS '点击数',
advs.adv_cli_num/advs.adv_show_num  AS '点击率'
FROM ADVERTISING_SPACE AS advs
LEFT JOIN ADVERTISEMENT_INFO AS advi ON advs.adv_id = advi.advertise_id
LEFT JOIN ACQUISITION_PLAN AS acqp ON advs.plan_id = acqp.acquisition_id
LEFT JOIN APP_INFO AS app_adv ON advs.adv_app_id = app_adv.app_id
LEFT JOIN APP_INFO AS app_plan ON advs.plan_app_id = app_plan.app_id
WHERE advi.advertise_name IS NOT NULL AND acqp.`name` IS NOT NULL AND app_adv.app_name IS NOT NULL AND app_plan.app_name IS NOT NULL
AND advs.plan_id != 0
AND advs.report_date BETWEEN @year_date_s AND @year_date_e
GROUP BY advs.report_date,app_adv.app_name,advi.advertise_name,app_plan.app_name,acqp.`name`,advs.adv_iss_num,advs.adv_show_num,advs.adv_cli_num
ORDER BY advs.report_date DESC
;

SELECT
advs.report_date        AS '交换时间',
app_adv.app_name        AS '广告位App',
advi.advertise_name     AS '广告位名称',
sum(advs.adv_req_num)   AS '请求数'
FROM ADVERTISING_SPACE  AS advs
LEFT JOIN ADVERTISEMENT_INFO AS advi ON advs.adv_id = advi.advertise_id
LEFT JOIN APP_INFO AS app_adv ON advs.adv_app_id = app_adv.app_id
WHERE advs.plan_app_id IS NOT NULL AND advs.plan_user_id IS NOT NULL
AND advs.report_date BETWEEN @year_date_s AND @year_date_e
GROUP BY advs.report_date,app_adv.app_name,advi.advertise_name
ORDER BY advs.report_date DESC
;





SELECT CURRENT_TIMESTAMP();











SELECT
advs.report_date        AS '交换时间',
app_adv.app_name        AS '广告位App',
sum(advs.adv_req_num)   AS '请求数',
sum(advs.adv_show_num)  AS '展示数',
sum(advs.adv_cli_num)   AS '点击数'
FROM ADVERTISING_SPACE  AS advs
LEFT JOIN ADVERTISEMENT_INFO AS advi ON advs.adv_id = advi.advertise_id
LEFT JOIN APP_INFO AS app_adv ON advs.adv_app_id = app_adv.app_id
WHERE advi.advertise_name IS NOT NULL AND app_adv.app_name IS NOT NULL
AND advs.plan_id != 0
AND advs.report_date BETWEEN @year_date_s AND @year_date_e
GROUP BY advs.report_date,app_adv.app_name
ORDER BY advs.report_date DESC
;


SELECT
advs.report_date    AS '交换时间',
app_plan.app_name   AS '获客计划App',
advs.adv_iss_num    AS '下发数'
FROM ADVERTISING_SPACE AS advs
LEFT JOIN ACQUISITION_PLAN AS acqp ON advs.plan_id = acqp.acquisition_id
LEFT JOIN APP_INFO AS app_plan ON advs.plan_app_id = app_plan.app_id
WHERE advs.plan_app_id IS NOT NULL AND advs.report_date BETWEEN @year_date_s AND @year_date_e
ORDER BY advs.report_date DESC
;






SELECT exchange_id,apply_app_id,apply_adver_id,apply_plan_id,`status`,audit_plan_id,audit_adver_id,audit_app_id,'----------------------' AS space,audit_user_id,apply_user_id
FROM EXCHANGE_INFO
WHERE (apply_adver_id = '2tMveHpPfG9bbpB4Q2gbRq' AND audit_plan_id = 421 AND audit_adver_id = 'Bxg8EjYEAfbDMdNhvAByzn')
or    (audit_adver_id = '2tMveHpPfG9bbpB4Q2gbRq' AND apply_plan_id = 421 AND apply_adver_id = 'Bxg8EjYEAfbDMdNhvAByzn')
;



-- 换量信息统计
DROP VIEW IF EXISTS ADV_COUNT_TMP;

CREATE VIEW ADV_COUNT_TMP AS SELECT
cast(advs.report_date AS date)      AS report_date,
app_adv.app_name                    AS adv_appname,
advs.adv_id                         AS adv_id,
advi.advertise_name                 AS adv_name,
advi.top_limit                      AS adv_show_max,
advi.real_total                     AS adv_show_count,
CASE exin.`status`
  WHEN 1 THEN CONCAT('申请人 ',app_apply.app_name,' 已申请')
  WHEN 2 THEN CONCAT('审核人 ',app_audit.app_name,' 审核中')
  WHEN 3 THEN CONCAT('审核人 ',app_audit.app_name,' 审核不通过')
  WHEN 4 THEN CONCAT('审核人 ',app_audit.app_name,' 审核通过')
  WHEN 5 THEN CONCAT('申请人 ',app_apply.app_name,' 审核中')
  WHEN 6 THEN CONCAT('申请人 ',app_apply.app_name,' 审核不通过')
  WHEN 7 THEN '流量交换开始'
  WHEN 8 THEN CONCAT('审核人 ',app_audit.app_name,' 暂停流量交换')
  WHEN 9 THEN CONCAT('申请人 ',app_apply.app_name,' 暂停流量交换')
  WHEN 10 THEN CONCAT('申请人 ',app_apply.app_name,' 停止流量交换')
  WHEN 11 THEN CONCAT('审核人 ',app_audit.app_name,' 停止流量交换')
  WHEN 12 THEN '系统停止'
  WHEN 13 THEN CONCAT('申请人 ',app_apply.app_name,' 审核通过')
  WHEN 14 THEN '待生效(生效变为7)'
  WHEN 999 THEN '删除'
  ELSE exin.`status` END                        AS ex_status,
CASE
  WHEN advi.real_total < advi.top_limit AND acqp.sum_count < acqp.sum_budget THEN '换量正常'
  WHEN advi.real_total >= advi.top_limit AND acqp.sum_count < acqp.sum_budget THEN CONCAT('广告位方 ',app_adv.app_name,' 交换量达上限')
  WHEN advi.real_total < advi.top_limit AND acqp.sum_count >= acqp.sum_budget THEN CONCAT('获客计划方 ',app_plan.app_name,' 交换量达上限')
  WHEN advi.real_total >= advi.top_limit AND acqp.sum_count >= acqp.sum_budget THEN '换量双方交换量均达上限'
END                                             AS ex_num,
app_plan.app_name                               AS plan_appname,
advs.plan_id                                    AS plan_id,
acqp.`name`                                     AS plan_name,
acqp.sum_budget                                 AS plan_show_max,
acqp.sum_count                                  AS plan_show_count,
sum(advs.adv_iss_num)                           AS iss_num,
sum(advs.adv_show_num)                          AS show_num,
sum(advs.adv_cli_num)                           AS cli_num,
sum(advs.adv_cli_num)/sum(advs.adv_show_num)    AS cli_show_rate
FROM ADVERTISING_SPACE        AS advs
LEFT JOIN ADVERTISEMENT_INFO  AS advi     ON advs.adv_id = advi.advertise_id
LEFT JOIN ACQUISITION_PLAN    AS acqp     ON advs.plan_id = acqp.acquisition_id
LEFT JOIN APP_INFO            AS app_adv  ON advs.adv_app_id = app_adv.app_id
LEFT JOIN APP_INFO            AS app_plan ON advs.plan_app_id = app_plan.app_id
LEFT JOIN EXCHANGE_INFO exin
ON (advs.adv_id = exin.audit_adver_id AND advs.plan_adv_id = exin.apply_adver_id AND advs.plan_id = exin.apply_plan_id)
OR (advs.adv_id = exin.apply_adver_id AND advs.plan_adv_id = exin.audit_adver_id AND advs.plan_id = exin.audit_plan_id)
LEFT JOIN APP_INFO AS app_apply ON exin.apply_app_id = app_apply.app_id
LEFT JOIN APP_INFO AS app_audit ON exin.audit_app_id = app_audit.app_id
WHERE advi.advertise_name IS NOT NULL AND acqp.`name` IS NOT NULL AND app_adv.app_name IS NOT NULL AND app_plan.app_name IS NOT NULL
AND advs.plan_id != 0
GROUP BY advs.report_date,app_adv.app_name,advi.advertise_name,app_plan.app_name,acqp.`name`,advs.adv_iss_num,advs.adv_show_num,advs.adv_cli_num;

DROP VIEW IF EXISTS EXCHANGE_INFO_COUNT;

CREATE VIEW EXCHANGE_INFO_COUNT AS SELECT
report_date       AS  report_date,
CASE
  WHEN ex_status = '流量交换开始' AND ex_num = '换量正常' THEN '换量正常'
  WHEN ex_status != '流量交换开始' AND ex_num = '换量正常' THEN CONCAT('停止 : ',ex_status)
  WHEN ex_status = '流量交换开始' AND ex_num != '换量正常' THEN CONCAT('停止 : ',ex_num)
  WHEN ex_status != '流量交换开始' AND ex_num != '换量正常' THEN CONCAT('停止 : 数量达阈值 ',ex_num,'，状态 ',ex_status)
END               AS  ex_status,
adv_appname       AS  adv_appname,
adv_name          AS  adv_name,
plan_appname      AS  plan_appname,
plan_name         AS  plan_name,
iss_num           AS  iss_num,
show_num          AS  show_num,
cli_num           AS  cli_num,
cli_show_rate     AS  cli_show_rate,
adv_id            AS  adv_id,
plan_id           AS  plan_id,
adv_show_max      AS  adv_show_max,
adv_show_count    AS  adv_show_count,
plan_show_max     AS  plan_show_max,
plan_show_count   AS  plan_show_count
FROM ADV_COUNT_TMP;




SELECT
report_date       AS  '交换时间',
ex_status         AS  '交换情况',
adv_appname       AS  '广告位App',
adv_name          AS  '广告位名称',
plan_appname      AS  '获客计划App',
plan_name         AS  '获客计划名称',
iss_num           AS  '下发数',
show_num          AS  '展示数',
cli_num           AS  '点击数',
cli_show_rate     AS  '点击率',
adv_id            AS  '广告位编号',
plan_id           AS  '获客计划编号',
adv_show_max      AS  '广告位展示上限',
adv_show_count    AS  '广告位交换数量',
plan_show_max     AS  '获客展示上限',
plan_show_count   AS  '获客交换数量'
FROM EXCHANGE_INFO_COUNT
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ adv_appname }} ]]
[[ AND {{ plan_appname }} ]]
ORDER BY report_date DESC

SELECT
report_date     AS  '交换时间',
ex_status       AS  '交换情况',
adv_appname     AS  '广告位App',
adv_name        AS  '广告位名称',
plan_appname    AS  '获客计划App',
plan_name       AS  '获客计划名称',
iss_num         AS  '下发数',
show_num        AS  '展示数',
cli_num         AS  '点击数',
cli_show_rate   AS  '点击率'
FROM EXCHANGE_INFO_COUNT
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ adv_appname }} ]]
[[ AND {{ plan_appname }} ]]
ORDER BY report_date DESC




-- 换量信息统计 -- 请求
DROP VIEW IF EXISTS EXCHANGE_INFO_REQ;

CREATE VIEW EXCHANGE_INFO_REQ AS
SELECT
cast(advs.report_date AS date)  AS  report_date,
app_adv.app_name                AS  app_name,
advs.adv_app_id                 AS  app_id,
advi.advertise_name             AS  adv_name,
advs.adv_id                     AS  adv_id,
sum(advs.adv_req_num)           AS  req_num
FROM ADVERTISING_SPACE          AS  advs
LEFT JOIN ADVERTISEMENT_INFO    AS  advi     ON advs.adv_id = advi.advertise_id
LEFT JOIN APP_INFO              AS  app_adv  ON advs.adv_app_id = app_adv.app_id
GROUP BY advs.report_date,app_adv.app_name,advi.advertise_name;


SELECT
report_date   AS  '交换时间',
app_name      AS  '广告位App',
app_id        AS  '广告位AppId',
adv_name      AS  '广告位名称',
adv_id        AS  '广告位Id',
req_num       AS  '请求数'
FROM EXCHANGE_INFO_REQ
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ app_name }} ]]
ORDER BY report_date DESC

SELECT
report_date   AS  '交换时间',
app_name      AS  '广告位App',
adv_name      AS  '广告位名称',
req_num       AS  '请求数'
FROM EXCHANGE_INFO_REQ
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ app_name }} ]]
ORDER BY report_date DESC



-- 反欺诈(管理员)总计
DROP VIEW IF EXISTS ADMIN_ADT_TOTAL;

CREATE VIEW ADMIN_ADT_TOTAL AS
SELECT
cast(report_date AS date) AS report_date,
app_name,
request_sum,
sum(device_exce + device_good + device_gene + device_diff + device_erro) AS device_sum,
sum(iprate_exce + iprate_gene + iprate_diff + iprate_erro) AS ip_sum
FROM UNFRAUD_RECOMMEND_WEFIX
LEFT JOIN APP_INFO ON UNFRAUD_RECOMMEND_WEFIX.app_id = APP_INFO.app_id
GROUP BY report_date,APP_INFO.app_name,request_sum;


SELECT
report_date   AS  '拦截时间',
app_name      AS  '拦截应用',
request_sum   AS  '总请求数',
device_sum    AS  '设备请求数',
ip_sum        AS  'IP请求数',
CASE
WHEN request_sum = device_sum AND request_sum = ip_sum THEN '数值相等'
WHEN request_sum != device_sum AND device_sum = ip_sum AND request_sum > device_sum THEN '请求数较大'
WHEN request_sum != device_sum AND device_sum = ip_sum AND request_sum < device_sum THEN '请求数较小'
WHEN request_sum != device_sum AND request_sum = ip_sum AND device_sum > request_sum THEN '设备请求数较大'
WHEN request_sum != device_sum AND request_sum = ip_sum AND device_sum < request_sum THEN '设备请求数较小'
WHEN request_sum = device_sum AND device_sum != ip_sum AND ip_sum > device_sum THEN 'IP请求数较大'
WHEN request_sum = device_sum AND device_sum != ip_sum AND ip_sum < device_sum THEN 'IP请求数较小'
ELSE '三组数都不相等' END  AS  '判断数值相等'
FROM ADMIN_ADT_TOTAL
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ app_name }} ]]
ORDER BY report_date DESC






-- 反欺诈信息(管理员)
DROP VIEW IF EXISTS ADMIN_ADT_DETAIL;

CREATE VIEW ADMIN_ADT_DETAIL AS
SELECT
cast(report_date AS date) AS report_date,
app_name,
request_sum,
blacklist,
device_exce,
device_good,
device_gene,
device_diff,
device_erro,
iprate_exce,
iprate_gene,
iprate_diff,
iprate_erro
FROM UNFRAUD_RECOMMEND_WEFIX
LEFT JOIN APP_INFO ON UNFRAUD_RECOMMEND_WEFIX.app_id = APP_INFO.app_id;



-- 反欺诈信息(管理员)
SELECT
report_date                             AS  '拦截时间',
viewer_appname                          AS  '拦截应用',
sum(cnt_b)                              AS  '总请求数',
sum(if(inblacklist = true,cnt_b,0))     AS  '黑名单数',
sum(if(quality_d = '优',cnt_d,0))       AS  '设备为优',
sum(if(quality_d = '良',cnt_d,0))       AS  '设备为良',
sum(if(quality_d = '一般',cnt_d,0))     AS  '设备为中',
sum(if(quality_d = '差',cnt_d,0))       AS  '设备为差',
sum(if(quality_d = 'NULL',cnt_d,0))     AS  '设备为烂',
sum(if(quality_i = '正常',cnt_i,0))     AS  '网段正常',
sum(if(quality_i = '一般',cnt_i,0))     AS  '网段一般',
sum(if(quality_i = '可疑',cnt_i,0))     AS  '网段可疑',
sum(if(quality_i = 'NULL',cnt_i,0))     AS  '网段为烂'
FROM ADT_ADMIN
WHERE 1 = 1
-- [[ AND {{ report_date }} ]]
-- [[ AND {{ viewer_appname }} ]]
GROUP BY report_date,viewer_appname
ORDER BY report_date DESC,viewer_appname






-- 反欺诈信息(用户)
DROP VIEW IF EXISTS ADT_USER_DETAIL;

CREATE VIEW ADT_USER_DETAIL AS
SELECT
cast(report_date AS date)   AS  report_date,
app_login.app_name          AS  login_appname,
adv_login.advertise_name    AS  login_advname,
app_viewer.app_name         AS  viewer_appname,
adv_viewer.advertise_name   AS  viewer_advname,
req_sum,
blacklist_sum,
sus_device_sum,
sus_ip_sum,
bl_device_sum,
bl_ip_sum,
dvi_ip_sum,
bl_dvi_ip_sum
FROM ADT_DATA
LEFT JOIN APP_INFO AS app_login ON ADT_DATA.login_appId = app_login.app_id
LEFT JOIN APP_INFO AS app_viewer ON ADT_DATA.viewer_appId = app_viewer.app_id
LEFT JOIN ADVERTISEMENT_INFO AS adv_login ON ADT_DATA.login_advId = adv_login.advertise_id
LEFT JOIN ADVERTISEMENT_INFO AS adv_viewer ON ADT_DATA.viewer_advId = adv_viewer.advertise_id;


SELECT
report_date       AS  '拦截时间',
login_appname     AS  '登录者应用',
login_advname     AS  '登录者广告位',
viewer_appname    AS  '展示者应用',
viewer_advname    AS  '展示者广告位',
req_sum           AS  '总请求数',
blacklist_sum     AS  '黑名单数量',
sus_device_sum    AS  '可疑设备数',
sus_ip_sum        AS  '可疑网段数',
bl_device_sum     AS  '黑名单&可疑设备数',
bl_ip_sum         AS  '黑名单&可疑网段数',
dvi_ip_sum        AS  '可疑设备&可疑网段数',
bl_dvi_ip_sum     AS  '黑名单&可疑设备&可疑网段数'
FROM ADT_USER_DETAIL
WHERE 1 = 1
[[ AND {{ report_date }} ]]
[[ AND {{ login_appname }} ]]
[[ AND {{ viewer_appname }} ]]
ORDER BY report_date DESC,login_appname,viewer_appname



https://dataauth.w-fix.com/validate/data/auth/dashboard?view=9@@@@test.json@@200002

https://dataauth.w-fix.com/validate/data/auth/question?view=4@@@@test.json@@200002


INSERT INTO ADT_DATA
(report_date,login_userId,login_appId,login_advId,viewer_appId,viewer_advId,req_sum,blacklist_sum,sus_device_sum,sus_ip_sum,bl_device_sum,bl_ip_sum,dvi_ip_sum,bl_dvi_ip_sum)
VALUES
(20191212,'7aef56be-184b-401b-86fb-1e2a834bf7ed','2','3','2JWWn8YHun8GTmZZ79Fw4A','1',10,20,30,40,50,60,70,80),
(20191212,'7aef56be-184b-401b-86fb-1e2a834bf7ed','2','5','2JWWn8YHun8GTmZZ79Fw4A','2',15,25,35,45,55,65,75,85),
(20191212,'7aef56be-184b-401b-86fb-1e2a834bf7ed','4','2','2JWWn8YHun8GTmZZ79Fw4A','6',15,25,35,45,55,65,75,85),
(20191215,'7aef56be-184b-401b-86fb-1e2a834bf7ed','','','','',0,0,0,0,0,0,0,0),
(20191225,'b6d433eb-2387-4c1a-acd3-a017b4fd69c4',1,3,'15vkdtYZcQVckj5JdBxrtP',2,10,1,1,1,1,1,1,1),
(20191225,'7aef56be-184b-401b-86fb-1e2a834bf7ed',1,1,'15vkdtYZcQVckj5JdBxrtP',1,55,5,2,5,2,2,2,2);




