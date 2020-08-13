set var:deal_date=2020-01-01;



/**
 * 对比ecif_id
 */
select ecif_no,id_no,if(decrypt_aes(id_no) = '422322196111160018','yes','no') as truefalse
from ecif_core.ecif_customer where id_no = encrypt_aes('422322196111160018') limit 1;


/**
 * 滴滴报文解析
 */
-- impala 执行
with
ecif  as (select ecif_no,id_no from ecif_core.ecif_customer),
dd_credit_apply as (
  select org,deal_date,create_time,update_time,
    get_json_object(original_msg,'$.didiRcFeature')                   as didircfeaturejson,
    get_json_object(original_msg,'$.flowNo')                          as flowno,
    get_json_object(original_msg,'$.signType')                        as signtype,
    get_json_object(original_msg,'$.applicationId')                   as applicationid,
    get_json_object(original_msg,'$.creditResultStatus')              as creditresultstatus,
    get_json_object(original_msg,'$.applySource')                     as applysource,
    get_json_object(original_msg,'$.userInfo.name')                   as name,
    get_json_object(original_msg,'$.userInfo.cardNo')                 as cardno,
    get_json_object(original_msg,'$.userInfo.phone')                  as phone,
    get_json_object(original_msg,'$.userInfo.telephone')              as telephone,
    get_json_object(original_msg,'$.userInfo.bankCardNo')             as bankcardno,
    get_json_object(original_msg,'$.userInfo.userRole')               as userrole,
    get_json_object(original_msg,'$.userInfo.idCardValidDate')        as idcardvaliddate,
    get_json_object(original_msg,'$.userInfo.address')                as address,
    get_json_object(original_msg,'$.userInfo.ocrInfo')                as ocrinfo,
    get_json_object(original_msg,'$.userInfo.idCardBackInfo')         as idcardbackinfo,
    get_json_object(original_msg,'$.userInfo.imageType')              as imagetype,
    get_json_object(original_msg,'$.userInfo.livingImageInfo')        as livingimageinfo,
    get_json_object(original_msg,'$.userInfo.sftpDir')                as sftpdir,
    get_json_object(original_msg,'$.creditInfo.amount')               as amount,
    get_json_object(original_msg,'$.creditInfo.interestRate')         as interestrate,
    get_json_object(original_msg,'$.creditInfo.interestPenaltyRate')  as interestpenaltyrate,
    get_json_object(original_msg,'$.creditInfo.startDate')            as startdate,
    get_json_object(original_msg,'$.creditInfo.endDate')              as enddate,
    get_json_object(original_msg,'$.creditInfo.lockDownEndTime')      as lockdownendtime
  from (
    -- 必须使用 regexp_replace ，因为源数据中有 '\' 。
    -- hive
    -- select
    --   org,deal_date,create_time,update_time,
    --   regexp_replace(
    --     regexp_replace(
    --       regexp_replace(
    --         original_msg,'\\\\',''
    --         ),'\\\"\\\{','\\\{'
    --       ),'\\\}\\\"','\\\}'
    --     ) as original_msg
    -- impala
    select
      org,deal_date,create_time,update_time,
      regexp_replace(
        regexp_replace(
          regexp_replace(
            original_msg,'\\\\',''
            ),'\"\{','\{'
          ),'\}\"','\}'
        ) as original_msg
    from ods.ecas_msg_log
    where msg_type = 'CREDIT_APPLY'
    -- and deal_date <= '${VAR:deal_date}'
  ) as a
  limit 10;
),
dd_loan_apply as (
  select
  get_json_object(original_msg,'$.creditId')                          as creditid,
  get_json_object(original_msg,'$.withdrawContactInfo')               as withdrawcontactinfo
  from (
    -- select regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}') as original_msg  -- hive
    select regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\"\{','\{'),'\}\"','\}') as original_msg -- impala
    from ods.ecas_msg_log
    where msg_type = 'LOAN_APPLY'
    -- and deal_date <= '${VAR:deal_date}'
  ) as b
)
-- INSERT OVERWRITE TABLE dwb.dwb_dd_log_detail partition(d_date)
select
  ecif.ecif_no        as ecif,
  name                as name,
  cardno              as cardno,
  null                as idtype,
  phone               as phone,
  telephone           as telephone,
  bankcardno          as bankcardno,
  userrole            as userrole,
  idcardvaliddate     as idcardvaliddate,
  address             as address,
  ocrinfo             as ocrinfo,
  idcardbackinfo      as idcardbackinfo,
  imagetype           as imagetype,
  null                as imagestatus,
  null                as imageurl,
  livingimageinfo     as livingimageinfo,
  sftpdir             as sftpdir,
  null                as sftp,
  amount              as amount,
  interestrate        as interestrate,
  interestpenaltyrate as interestpenaltyrate,
  startdate           as startdate,
  enddate             as enddate,
  lockdownendtime     as lockdownendtime,
  didircfeaturejson   as didircfeaturejson,
  flowno              as flowno,
  signtype            as signtype,
  applicationid       as applicationid,
  creditresultstatus  as creditresultstatus,
  applysource         as applysource,
  deal_date           as deal_date,
  create_time         as create_time,
  update_time         as update_time,
  withdrawcontactinfo as linkman_info,
  org                 as org,
  '${VAR:deal_date}'  as d_date
from dd_credit_apply
left join ecif on encrypt_aes(dd_credit_apply.cardno) = ecif.id_no
left join dd_loan_apply on dd_credit_apply.applicationId = dd_loan_apply.creditid;

select idtype,imageStatus,imageUrl,sftp,linkman_info from dwb.dwb_dd_log_detail limit 10;

/**
 * 业务数据库客户信息表
 */
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_bussiness_customer_info partition(d_date)
select
  org                 as org,
  channel             as channel,
  cust_id             as cust_id,
  outer_cust_id       as outer_cust_id,
  ecas_customer.id_no as id_no,
  id_type             as id_type,
  name                as name,
  mobie               as mobie,
  cust_lmt_id         as cust_lmt_id,
  create_time         as create_time,
  create_user         as create_user,
  jpa_version         as jpa_version,
  overflow_amt        as overflow_amt,
  gender              as gender,
  bir_date            as bir_date,
  marital_status      as marital_status,
  permanent_address   as permanent_address,
  now_address         as now_address,
  bank_no             as bank_no,
  lst_upd_time        as lst_upd_time,
  lst_upd_user        as lst_upd_user,
  apply_no            as apply_no,
  city                as city,
  job_type            as job_type,
  province            as province,
  country             as country,
  ecif_no             as ecif_id,
  '${VAR:deal_date}'  as d_date
from (
  select * from ods.ecas_customer
  -- where d_date = '${VAR:deal_date}'
) as ecas_customer
left join (select ecif_no,id_no from ecif_core.ecif_customer) as ecif_customer on encrypt_aes(ecas_customer.id_no) = ecif_customer.id_no;


/**
 * 银行卡信息
 */
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_bank_card_info partition(d_date)
select
  card_id               as card_id,
  cust_id               as cust_id,
  due_bill_no           as due_bill_no,
  bank_card_id_no       as bank_card_id_no,
  bank_card_name        as bank_card_name,
  bank_card_phone       as bank_card_phone,
  pay_channel           as pay_channel,
  card_flag             as card_flag,
  agreement_no          as agreement_no,
  bank_card_no          as bank_cardno,
  NULL                  as bank_no,   -- 值有 NULL 和 ''
  NULL                  as bank_name, -- 值有 NULL 和 ''
  NULL                  as province,  -- 值有 NULL 和 ''
  NULL                  as city,      -- 值有 NULL 和 ''
  create_time           as tied_card_time,
  ecif_customer.ecif_no as ecif,
  org                   as org,
  '${VAR:deal_date}'    as d_date
from (
  select * from ods.ecas_bind_card
  -- where d_date = '${VAR:deal_date}'
) as bind_card
left join (select ecif_no,id_no from ecif_core.ecif_customer) as ecif_customer on encrypt_aes(bind_card.bank_card_id_no) = ecif_customer.id_no;

select * from ods.ecas_bind_card limit 20;
select * from dwb.dwb_bank_card_info limit 20;

select distinct bank_no,bank_name,province,city from dwb.dwb_bank_card_info limit 200;


/**
 * 银行卡变更信息表
 */
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_bind_card_change partition(d_date)
select
  change_id                 as change_id,
  org                       as org,
  cust_id                   as cust_id,
  due_bill_no               as due_bill_no,
  old_bank_card_id_no       as old_bank_card_id_no,
  old_bank_card_name        as old_bank_card_name,
  old_bank_card_phone       as old_bank_card_phone,
  old_bank_card_no          as old_bank_card_no,
  old_pay_channel           as old_pay_channel,
  old_agreement_no          as old_agreement_no,
  old_card_flag             as old_card_flag,
  new_bank_card_id_no       as new_bank_card_id_no,
  new_bank_card_name        as new_bank_card_name,
  new_bank_card_phone       as new_bank_card_phone,
  new_bank_card_no          as new_bank_card_no,
  new_pay_channel           as new_pay_channel,
  new_agreement_no          as new_agreement_no,
  new_card_flag             as new_card_flag,
  create_time               as create_time,
  lst_upd_time              as lst_upd_time,
  jpa_version               as jpa_version,
  ecif_customer_old.ecif_no as old_ecif,
  ecif_customer_new.ecif_no as new_ecif,
  '${VAR:deal_date}'        as d_date
from (
  select * from ods.ecas_bind_card_change
  -- where d_date = '${VAR:deal_date}'
) as bind_card_change
left join (select ecif_no,id_no from ecif_core.ecif_customer) as ecif_customer_old on encrypt_aes(bind_card_change.old_bank_card_id_no) = ecif_customer_old.id_no
left join (select ecif_no,id_no from ecif_core.ecif_customer) as ecif_customer_new on encrypt_aes(bind_card_change.new_bank_card_id_no) = ecif_customer_new.id_no;


select * from ods.ecas_bind_card_change limit 20;
select * from dwb.dwb_bind_card_change limit 20;

select distinct due_bill_no,old_pay_channel,old_agreement_no,old_card_flag,new_pay_channel,new_agreement_no,new_card_flag from dwb.dwb_bind_card_change limit 200;



/**
 * 授信信息变更表
 */
select * from ods.ecas_msg_log where deal_date <= '"+partitionDate+"' and msg_type='CREDIT_CHANGE'




/**
 * 老账户核心实还表
*/
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_repay_hst partition(p_type)
select
  ccs_repay_hst.payment_id                                                   as payment_id,
  ccs_loan.loan_id                                                           as loan_id,
  ccs_repay_hst.acct_nbr                                                     as due_bill_no,
  case
  when ccs_repay_hst.bnp_type = 'pastInterest' or ccs_repay_hst.bnp_type = 'ctdInterest'   then 'Interest'
  when ccs_repay_hst.bnp_type = 'pastSvcFee'   or ccs_repay_hst.bnp_type = 'ctdSvcFee'     then 'SVCFee'
  when ccs_repay_hst.bnp_type = 'ctdPrincipal' or ccs_repay_hst.bnp_type = 'pastPrincipal' then 'Pricinpal'
  when ccs_repay_hst.bnp_type = 'pastPenalty'  or ccs_repay_hst.bnp_type = 'ctdPenalty'    then 'Penalty'
  else ccs_repay_hst.bnp_type end                                            as bnp_type,
  cast(ccs_repay_hst.repay_amt as decimal(15,2))                             as repay_amt,
  ccs_repay_hst.batch_date                                                   as batch_date,
  ccs_repay_hst.term                                                         as term,
  ccs_repay_hst.order_id                                                     as order_id,
  ccs_repay_hst.txn_seq                                                      as txn_seq,
  from_unixtime(cast(ccs_repay_hst.create_time/1000 as bigint),'yyyy-MM-dd') as txn_date,
  ccs_loan.loan_status                                                       as loan_status,
  'ccs'                                                                      as p_type
from (
  select
    payment_id,
    acct_nbr,
    bnp_type,
    repay_amt,
    batch_date,
    term,
    order_id,
    txn_seq,
    create_time
  from ods.ccs_repay_hst
  -- where d_date = '${VAR:deal_date}'
) as ccs_repay_hst
left join (
  select
    loan_id,
    acct_nbr,
    loan_status
  from ods.ccs_loan
  -- where d_date = '${VAR:deal_date}'
) as ccs_loan on ccs_repay_hst.acct_nbr = ccs_loan.acct_nbr;



/**
 * 借据信息表
 */
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_loan partition(d_date,p_type)
select
  ecif_id,
  loan_id,
  channel_id,
  capital_id,
  product_code,
  due_bill_no,
  register_date,
  loan_init_principal,
  loan_init_term,
  loan_init_interest,
  loan_init_fee,
  remain_principal,
  remain_interest,
  loan_principal,
  loan_interest,
  loan_penalty,
  loan_fee,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_fee,
  overdue_principal,
  overdue_interest,
  overdue_fee,
  min(curr_term)  as curr_term,
  paid_out_date,
  overdue_date,
  overdue_days,
  cpd_begin_date,
  loan_expire_date,
  interest_rate,
  fee_rate,
  penalty_rate,
  contr_nbr,
  loan_usage,
  application_no,
  loan_status,
  create_time,
  loan_terminal_code,
  paid_out_type,
  flag,
  cycle_day,
  user_field1,
  d_date,
  p_type
from (
  select
    loan_id             as loan_id,
    acq_id              as channel_id,
    capital_plan_no     as capital_id,
    product_code        as product_code,
    due_bill_no         as due_bill_no,
    active_date         as register_date,
    loan_init_prin      as loan_init_principal,
    loan_init_term      as loan_init_term,
    totle_int           as loan_init_interest,
    totle_svc_fee       as loan_init_fee,
    0                   as remain_principal,
    0                   as remain_interest,
    loan_init_prin      as loan_principal,
    totle_int           as loan_interest,
    totle_penalty       as loan_penalty,
    totle_svc_fee       as loan_fee,
    paid_principal      as paid_principal,
    paid_interest       as paid_interest,
    paid_penalty        as paid_penalty,
    paid_svc_fee        as paid_fee,
    overdue_prin        as overdue_principal,
    overdue_interest    as overdue_interest,
    overdue_svc_fee     as overdue_fee,
    paid_out_date       as paid_out_date,
    overdue_date        as overdue_date,
    overdue_days        as overdue_days,
    cpd_begin_date      as cpd_begin_date,
    loan_expire_date    as loan_expire_date,
    interest_rate       as interest_rate,
    svc_fee_rate        as fee_rate,
    penalty_rate        as penalty_rate,
    contract_no         as contr_nbr,
    purpose             as loan_usage,
    apply_no            as application_no,
    loan_status         as loan_status,
    create_time         as create_time,
    terminal_reason_cd  as loan_terminal_code,
    null                as flag,
    cycle_day           as cycle_day,
    null                as user_field1,
    '${VAR:deal_date}'  as d_date,
    case acq_id
      when '10000' then 'DD'
      when '0003'  then 'HT'
      when '0006'  then 'LX'
    else 'error' end    as p_type
  from ods.ecas_loan
  -- where d_date = '${VAR:deal_date}'
) as ecas_loan
left join (
  select
    curr_term,
    due_bill_no as bill_no,
    paid_out_type
  from ods.ecas_repay_schedule
  where curr_term > 0
  -- and d_date = '${VAR:deal_date}' and pmt_due_date >= '${VAR:deal_date}'
) as ecas_repay_schedule on ecas_loan.due_bill_no = ecas_repay_schedule.bill_no
left join (
  select attr_value,ecif_id
  from (select inner_id,attr_value from ecif_core.ecif_customer_attribute where attr_key = 'application_no') as a
  join (select inner_id,ecif_no as ecif_id from ecif_core.ecif_inner_id) as b on a.inner_id = b.inner_id
) as ecif on ecas_loan.application_no = ecif.attr_value
group by ecif_id,
  loan_id,
  channel_id,
  capital_id,
  product_code,
  due_bill_no,
  register_date,
  loan_init_principal,
  loan_init_term,
  loan_init_interest,
  loan_init_fee,
  remain_principal,
  remain_interest,
  loan_principal,
  loan_interest,
  loan_penalty,
  loan_fee,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_fee,
  overdue_principal,
  overdue_interest,
  overdue_fee,
  paid_out_date,
  overdue_date,
  overdue_days,
  cpd_begin_date,
  loan_expire_date,
  interest_rate,
  fee_rate,
  penalty_rate,
  contr_nbr,
  loan_usage,
  application_no,
  loan_status,
  create_time,
  loan_terminal_code,
  paid_out_type,
  flag,
  cycle_day,
  user_field1,
  d_date,
  p_type;





/**
 * 凤金ccs_order
 */
-- impala 执行
-- INSERT OVERWRITE TABLE dwb.dwb_order partition(d_date,p_type)
select
  ecif_no             as ecif_id,
  channel_id          as channel_id,
  capital_id          as capital_id,
  order_id            as order_id,
  loan_id             as loan_id,
  due_bill_no         as due_bill_no,
  application_no      as apply_no,
  product_code        as product_code,
  service_id          as service_id,
  command_type        as command_type,
  purpose             as purpose,
  business_date       as business_date,
  term                as term,
  order_time          as order_time,
  order_status        as order_status,
  loan_usage          as loan_usage,
  null                as repay_way,
  txn_amt             as txn_amt,
  null                as bank_trade_no,
  null                as bank_acct,
  null                as bank_acct_name,
  memo                as memo,
  code                as code,
  message             as message,
  null                as flag,
  '${VAR:deal_date}'  as d_date,
  'ccs'               as p_type
from (
  select
    order_id,command_type,order_status,order_time,txn_amt,purpose,code,
    message,due_bill_no,business_date,loan_usage,memo,contr_nbr,service_id,term
  from ods.ccs_order
  -- where d_date = '${VAR:deal_date}'
  union
  select
    order_id,command_type,order_status,order_time,txn_amt,purpose,code,
    message,due_bill_no,business_date,loan_usage,memo,contr_nbr,service_id,term
  from ods.ccs_order_hst
  -- where d_date = '${VAR:deal_date}'
) as ccs_order
left join (
  select loan_id,due_bill_no,channel_id,capital_id,product_code,application_no from dwb.dwb_loan
  -- where d_date = '${VAR:deal_date}
) as dwb_loan on ccs_order.due_bill_no = dwb_loan.due_bill_no
left join (
  select attr_value,ecif_no
  from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute where attr_key = 'application_no' ) as a
  join ( select inner_id,ecif_no from ecif_core.ecif_inner_id ) as b on a.inner_id = b.inner_id
) as ecif on dwb_loan.application_no = ecif.attr_value
union all
select
  ecif.ecif_no                     as ecif_id,
  dwb_loan.channel_id              as channel_id,
  dwb_loan.capital_id              as capital_id,
  ecas_order.order_id              as order_id,
  dwb_loan.loan_id                 as loan_id,
  ecas_order.due_bill_no           as due_bill_no,
  ecas_order.apply_no              as apply_no,
  dwb_loan.product_code            as product_code,
  ecas_order.service_id            as service_id,
  ecas_order.command_type          as command_type,
  ecas_order.purpose               as purpose,
  ecas_order.business_date         as business_date,
  ecas_order.term                  as term,
  ecas_order.order_time            as order_time,
  ecas_order.order_status          as order_status,
  ecas_order.loan_usage            as loan_usage,
  ''                               as repay_way,
  ecas_order.txn_amt               as txn_amt,
  ecas_order.bank_trade_no         as bank_trade_no,
  ecas_order.bank_trade_act_no     as bank_acct,
  ecas_order.bank_trade_act_name   as bank_acct_name,
  ecas_order.memo                  as memo,
  ecas_order.code                  as code,
  ecas_order.message               as message,
  ''                               as flag,
  '${VAR:deal_date}'               as d_date,
  'ecas'                           as p_type
from (
  select
    order_id,
    command_type,
    order_status,
    order_time,
    txn_amt,
    purpose,
    code,
    message,
    due_bill_no,
    business_date,
    loan_usage,
    memo,
    contr_nbr,
    service_id,
    term,
    apply_no,
    bank_trade_no,
    bank_trade_act_no,
    bank_trade_act_name,
    bank_trade_act_phone,
    txn_date
  from ods.ecas_order
  where d_date = '${VAR:deal_date}'
  union
  select
    order_id,
    command_type,
    order_status,
    order_time,
    txn_amt,
    purpose,
    code,
    message,
    due_bill_no,
    business_date,
    loan_usage,
    memo,
    contr_nbr,
    service_id,
    term,
    apply_no,
    bank_trade_no,
    bank_trade_act_no,
    bank_trade_act_name,
    bank_trade_act_phone,
    txn_date
  from ods.ecas_order_hst
  -- where d_date = '${VAR:deal_date}'
) as ecas_order
left join (
  select
    loan_id,
    due_bill_no,
    channel_id,
    capital_id,
    product_code
  from dwb.dwb_loan
  -- where d_date = '${VAR:deal_date}'
) as dwb_loan on ecas_order.due_bill_no = dwb_loan.due_bill_no
left join (
  select attr_value,ecif_no
  from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute where attr_key = 'application_no' ) as a
  join ( select inner_id,ecif_no from ecif_core.ecif_inner_id ) as b on a.inner_id = b.inner_id
) as ecif on ecas_order.apply_no = ecif.attr_value;








/**
 * 凤金项目 还款计划转换 滴滴格式
 */
-- impala 执行
set var:deal_date=2020-01-01;
with repay_schedule as (
  select
    ccs_loan.due_bill_no,
    ccs_loan.loan_id,
    ccs_loan.register_date,
    ccs_loan.paid_out_date,
    ccs_loan.ref_nbr,
    ccs_loan.loan_init_term,
    ccs_loan.terminal_reason_cd,
    ccs_repay_schedule.schedule_id,
    ccs_repay_schedule.curr_term as term,
    ccs_repay_schedule.loan_term_prin,
    ccs_repay_schedule.loan_term_int,
    ccs_repay_schedule.loan_svc_fee,
    loan_pmt_due_date,
    loan_grace_date
  from (
    select due_bill_no,loan_id,register_date,ref_nbr,paid_out_date,loan_init_term,terminal_reason_cd
    from ods.ccs_loan
    -- where d_date = '${VAR:deal_date}'
  ) as ccs_loan
  left join (
    select schedule_id,curr_term,loan_term_prin,loan_term_int,loan_svc_fee ,loan_pmt_due_date,loan_grace_date,ref_nbr
    from ods.ccs_repay_schedule
    -- where d_date = '${VAR:deal_date}'
  ) ccs_repay_schedule on ccs_loan.ref_nbr = ccs_repay_schedule.ref_nbr
)
-- INSERT OVERWRITE TABLE dwb.dwb_repay_schedule partition(d_date,p_type)
select
  schedule_id,
  loan_id,
  channel_id,
  capital_id,
  term,
  due_bill_no,
  loan_pmt_due_date,
  register_date,
  remain_principal,
  remain_interest,
  loan_principal,
  loan_interest,
  loan_penalty,
  loan_fee,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_fee,
  paid_mult,
  reduce_principal,
  reduce_interest,
  reduce_fee,
  reduce_penalty,
  reduce_mult,
  penalty_acru,
  paid_out_date,
  paid_out_type,
  schedule_status,
  grace_date,
  flag,
  -- '${VAR:deal_date}' as d_date，
  'FJ' as p_type
from (
  select
    schedule_id,
    loan_id,
    channel_id,
    null as capital_id,
    repay_part.term,
    due_bill_no,
    loan_pmt_due_date,
    register_date,
    remain_principal,
    remain_interest,
    loan_principal,
    loan_interest,
    repay_part.paid_penalty
    + if(ccsplandataset.ctd_penalty is null,0,ccsplandataset.ctd_penalty)
    + if(ccsplandataset.past_penalty is null,0,ccsplandataset.past_penalty) as loan_penalty,
    loan_fee,
    paid_principal,
    paid_interest,
    paid_fee,
    paid_penalty,
    paid_mult,
    reduce_principal,
    reduce_interest,
    reduce_penalty,
    reduce_fee,
    reduce_mult,
    if(ccsplandataset.penalty_acru is null,repay_part.penalty_acru,ccsplandataset.penalty_acru) as penalty_acru,
    if(ccsplandataset.paid_out_date is not null,ccsplandataset.paid_out_date,repay_part.paid_out_date) as paid_out_date,
    paid_out_type,
    if(!(ccsplandataset.term is not null and ccsplandataset.ref_nbr is not null),'N',if(ccsplandataset.paid_out_date is null,'O','F')) as schedule_status,
    grace_date,
    flag
  from (
    select
      repay_schedule.schedule_id,
      repay_schedule.loan_id,
      '0004'                         as channel_id,
      null                           as capital_id,
      repay_schedule.term,
      repay_schedule.due_bill_no,
      repay_schedule.loan_pmt_due_date,
      repay_schedule.register_date,
      0                              as remain_principal,
      0                              as remain_interest,
      repay_schedule.loan_term_prin  as loan_principal,
      repay_schedule.loan_term_int   as loan_interest,
      if(dwb_repay_hst.due_bill_no is not null and dwb_repay_hst.term is not null,dwb_repay_hst.paid_penalty,0)   as loan_penalty,
      repay_schedule.loan_svc_fee    as loan_fee,
      if(dwb_repay_hst.due_bill_no is not null and dwb_repay_hst.term is not null,dwb_repay_hst.paid_principal,0) as paid_principal,
      if(dwb_repay_hst.due_bill_no is not null and dwb_repay_hst.term is not null,dwb_repay_hst.paid_interest,0)  as paid_interest,
      if(dwb_repay_hst.due_bill_no is not null and dwb_repay_hst.term is not null,dwb_repay_hst.paid_fee,0)       as paid_fee,
      if(dwb_repay_hst.due_bill_no is not null and dwb_repay_hst.term is not null,dwb_repay_hst.paid_penalty,0)   as paid_penalty,
      0                              as paid_mult,
      0                              as reduce_principal,
      0                              as reduce_interest,
      0                              as reduce_penalty,
      0                              as reduce_fee,
      0                              as reduce_mult,
      0                              as penalty_acru,
      paid_out_date,
      null                           as paid_out_type,
      null                           as schedule_status,
      repay_schedule.loan_grace_date as grace_date,
      repay_schedule.ref_nbr         as flag -- 暂时用 flag 字段 存放 ref_nbr
    from (
      select
        schedule_id,
        loan_id,
        term,
        due_bill_no,
        loan_pmt_due_date,
        register_date,
        paid_out_date,
        ref_nbr,
        loan_init_term,
        terminal_reason_cd,
        loan_term_prin,
        loan_term_int,
        loan_svc_fee,
        loan_grace_date
      from repay_schedule
      where paid_out_date is null
    ) as repay_schedule
    left join (
      select
        due_bill_no,
        term,
        sum(if(bnp_type='Pricinpal',repay_amt,0)) as paid_principal,
        sum(if(bnp_type='Interest',repay_amt,0))  as paid_interest,
        sum(if(bnp_type='Penalty',repay_amt,0))   as paid_penalty,
        sum(if(bnp_type='SVCFee',repay_amt,0))    as paid_fee
      from dwb.dwb_repay_hst
      where p_type= 'ccs'
      -- and d_date <= '${VAR:deal_date}'
      group by due_bill_no,term
    ) as dwb_repay_hst on repay_schedule.due_bill_no = dwb_repay_hst.due_bill_no and repay_schedule.term = dwb_repay_hst.term
  ) as repay_part
  left join (
    select ref_nbr,term,paid_out_date,ctd_penalty,past_penalty,penalty_acru
    from ods.ccs_plan
    where plan_type = 'Q'
    -- and d_date = '${VAR:deal_date}'
  ) as ccsplandataset on repay_part.term = ccsplandataset.term and repay_part.flag = ccsplandataset.ref_nbr
  union
  select
    loan.schedule_id,
    loan.loan_id,
    schedule.channel_id,
    schedule.capital_id,
    loan.term,
    loan.due_bill_no,
    loan.loan_pmt_due_date,
    loan.register_date,
    schedule.remain_principal,
    schedule.remain_interest,
    schedule.loan_principal,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_interest) as loan_interest,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_penalty)  as loan_penalty,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_fee)      as loan_fee,
    schedule.paid_principal,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_interest) as paid_interest,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_penalty)  as paid_penalty,
    if(loan.terminal_reason_cd = 'F',0,schedule.loan_fee)      as paid_fee,
    schedule.paid_mult,
    schedule.reduce_principal,
    schedule.reduce_interest,
    schedule.reduce_fee,
    schedule.reduce_penalty,
    schedule.reduce_mult,
    schedule.penalty_acru,
    loan.paid_out_date                                         as paid_out_date,
    schedule.paid_out_type,
    schedule.schedule_status,
    schedule.grace_date,
    schedule.flag
  from (
    select
      schedule_id,
      loan_id,
      term,
      due_bill_no,
      loan_pmt_due_date,
      register_date,
      paid_out_date,
      ref_nbr,
      loan_init_term,
      terminal_reason_cd,
      loan_term_prin,
      loan_term_int,
      loan_svc_fee,
      loan_grace_date
    from repay_schedule
    where paid_out_date is not null
    -- and paid_out_date = '${VAR:deal_date}'
  ) as loan
  left join (
    select
      schedule_id,
      loan_id,
      channel_id,
      capital_id,
      term,
      due_bill_no,
      loan_pmt_due_date,
      register_date,
      remain_principal,
      remain_interest,
      loan_principal,
      loan_interest,
      loan_penalty,
      loan_fee,
      paid_principal,
      paid_interest,
      paid_penalty,
      paid_fee,
      paid_mult,
      reduce_principal,
      reduce_interest,
      reduce_fee,
      reduce_penalty,
      reduce_mult,
      penalty_acru,
      paid_out_date,
      paid_out_type,
      schedule_status,
      grace_date,
      flag
    from dwb.dwb_repay_schedule
    where p_type = 'FJ'
    -- and d_date = '${VAR:deal_date}'
  ) as schedule on loan.due_bill_no = schedule.due_bill_no and schedule.paid_out_date is null
  union
  select schedule.*
  from (
    select *
    from repay_schedule
    where paid_out_date is not null
    -- and paid_out_date < '${VAR:deal_date}'
  ) as loan
  left join (
    select
      schedule_id,
      loan_id,
      channel_id,
      capital_id,
      term,
      due_bill_no,
      loan_pmt_due_date,
      register_date,
      remain_principal,
      remain_interest,
      loan_principal,
      loan_interest,
      loan_penalty,
      loan_fee,
      paid_principal,
      paid_interest,
      paid_penalty,
      paid_fee,
      paid_mult,
      reduce_principal,
      reduce_interest,
      reduce_fee,
      reduce_penalty,
      reduce_mult,
      penalty_acru,
      paid_out_date,
      paid_out_type,
      schedule_status,
      grace_date,
      flag
    from dwb.dwb_repay_schedule
    where p_type='FJ'
    -- and d_date = '${VAR:deal_date}'
  ) as schedule on loan.due_bill_no = schedule.due_bill_no
) as base;




/**
 *
 */
-- impala 执行
with base as (
  select
    ecif_no            as ecif_id,
    loan_id            as loan_id,
    due_bill_no        as due_bill_no,
    active_date        as register_date,
    loan_status        as loan_status,
    paid_out_date      as paid_out_date,
    curr_term          as curr_term,
    remain_term        as remain_term,
    0                  as remain_interest,
    0                  as remain_principal,
    loan_init_term     as loan_init_term,
    loan_init_prin     as loan_init_principal,
    loan_init_prin     as loan_principal,
    totle_int          as loan_init_interest,
    totle_int          as loan_interest,
    totle_svc_fee      as loan_init_fee,
    totle_svc_fee      as loan_fee,
    totle_penalty      as loan_penalty,
    paid_principal     as paid_principal,
    paid_interest      as paid_interest,
    paid_svc_fee       as paid_fee,
    paid_penalty       as paid_penalty,
    paid_mult          as paid_mult,
    interest_rate      as interest_rate,
    penalty_rate       as penalty_rate,
    svc_fee_rate       as fee_rate,
    loan_expire_date   as loan_expire_date,
    contract_no        as contr_nbr,
    overdue_date       as overdue_date,
    overdue_days       as overdue_days,
    overdue_prin       as overdue_principal,
    overdue_interest   as overdue_interest,
    overdue_svc_fee    as overdue_fee,
    purpose            as loan_usage,
    product_code       as product_code,
    create_time        as create_time,
    loan_settle_reason as loan_settle_reason,
    acq_id             as channel_id,
    cycle_day          as cycle_day,
    capital_plan_no    as capital_id,
    capital_type       as capital_type,
    terminal_reason_cd as loan_terminal_code,
    cycle_day          as cycle_day,
    apply_no           as application_no
  from (
    select *
    from ods.ecas_loan
    -- where d_date = '${VAR:deal_date}'
  ) as ecas_loan
  left join (
    select attr_value,ecif_id
    from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute where attr_key = 'application_no' ) as a
    join ( select inner_id,ecif_no as ecif_id from ecif_core.ecif_inner_id ) as b on a.inner_id = b.inner_id
  ) as ecif on ecas_loan.apply_no = ecif.attr_value
)


select
a.due_bill_no as due_bill_no,
min(b.curr_term) as curr_term
from (select due_bill_no,curr_term from base) a
left join (
  select due_bill_no,pmt_due_date,curr_term
  from ods.ecas_repay_schedule
  where curr_term > 0
  -- and d_date = '${VAR:deal_date}'
) b
on a.due_bill_no=b.due_bill_no
where b.pmt_due_date >= '+partitionDate+' or a.curr_term = b.curr_term
group by a.due_bill_no;




-- LOAN_RESULT
-- LOAN_APPLY
-- BIND_BANK_CARD_CHANGE
-- CREDIT_APPLY
-- REPAY_RESULT
-- BIND_BANK_CARD
-- CREDIT_CHANGE

-- 授信详情表
-- dm.dm_watch_credit_detail
select distinct
  -- concat(credit_apply.apply_id,${VAR:deal_date})                                            as id,
  cast(credit_apply.ecif_id      as string)                                                 as ecif_no,
  cast(credit_apply.channel_id   as string)                                                 as channel_code,
  cast(credit_apply.product_code as string)                                                 as product_code,
  cast(if(credit_result.credit_result in (1,0,3),credit_result.credit_result,0) as int)     as is_credit_success,
  cast(if(credit_result.credit_result = 1,'通过',null) as string)                           as failure_msg,
  cast(from_unixtime(cast(credit_apply.credit_time/1000 as bigint),'yyyy-MM-dd') as string) as approval_time,
  cast(credit_result.amount      as decimal(15,4))                                          as approval_amount,
  cast(credit_apply.apply_amt    as decimal(15,4))                                          as apply_amount,
  credit_apply.apply_date                                                                   as apply_time,
  cast(credit_apply.apply_id     as string)                                                 as credit_id,
  to_date(credit_apply.end_date)                                                            as credit_validity,
  null                                                                                      as ext
  -- ,${VAR:deal_date}                                                                         as d_date
from (
  select
    ecif_id,
    channel_id,
    product_code,
    credit_time,
    apply_amt,
    apply_date,
    apply_id,
    end_date
  from dwb.dwb_credit_apply
  -- where process_date <= '${VAR:deal_date}'
) as credit_apply
left join (
  select cast(credit_result as tinyint) as credit_result,amount,apply_id
  from dwb.dwb_credit_result
  -- where d_date = '${VAR:deal_date}'
) as credit_result on credit_apply.apply_id = credit_result.apply_id
limit 10;




-- 授信信息快照表
-- dm.dm_watch_credit_info
select
  -- concat(credit_apply.apply_no,'+partitionDate+',if(loan_apply.due_bill_no is null,uuid(),loan_apply.due_bill_no))  as id,
  credit_apply.apply_no                                                                    as apply_no,
  loan_apply.due_bill_no                                                                   as bill_no,
  to_date(rc_result.assess_date)                                                           as assessment_date,
  case rc_result.process_result when 'Y' then '成功' when 'N' then '不通过' else NULL end  as credit_result,
  rc_result.amt                                                                            as credit_amount,
  credit_apply.end_date                                                                    as credit_validity,
  null                                                                                     as refuse_reason,
  credit_apply.process_date                                                                as snapshot_date,
  null                                                                                     as update_time,
  null                                                                                     as create_time
from (
  select apply_no,end_date,process_date from dwb.dwb_credit_apply
  -- where process_date <= '${VAR:deal_date}'
) as credit_apply
left join (select due_bill_no,apply_no from dwb.dwb_loan_apply
  -- where process_date <= '${VAR:deal_date}'
) as loan_apply
on credit_apply.apply_no = loan_apply.apply_no
left join (
  select assess_date,process_result,amt,application_no from dwb.dwb_risk_control_result
  -- where process_date <= '${VAR:deal_date}'
) as rc_result
on credit_apply.apply_no = rc_result.application_no
limit 10;





select
  credit_apply.process_date,
  credit_apply.apply_no,
  loan.loan_id,
  credit_apply.limit_amt - sum(
    if(loan.remain_principal is NULL,0,
      if(repay_schedule.loan_id is NULL,loan.remain_principal,repay_schedule.current_remaining_principal)
      )
    ) as remain_principal
from (
  select apply_no,limit_amt,process_date from dwb.dwb_credit_apply
  -- where process_date <= '${VAR:deal_date}'
) as credit_apply
left join (
  select application_no,loan_id,remain_principal
  from dwb.dwb_loan
  -- where d_date = '${VAR:deal_date}'
) as loan on credit_apply.apply_no = loan.application_no
left join (
  select loan_id,sum(if(paid_out_date is null,loan_principal - paid_principal,0)) as current_remaining_principal
  from dwb.dwb_repay_schedule
  -- where d_date = '${VAR:deal_date}'
  group by loan_id
) as repay_schedule on loan.loan_id = repay_schedule.loan_id
group by credit_apply.apply_no,loan.loan_id,credit_apply.limit_amt,credit_apply.process_date
limit 100;





select
credit_apply.apply_no,
cast(
  if((loan.accu_used_amt / credit_apply.limit_amt) * 100 is null,0,(loan.accu_used_amt / credit_apply.limit_amt) * 100)
  as decimal(10,2)) as accumulate_credit_amount_utilization_rate
from (
  select apply_no,limit_amt from dwb.dwb_credit_apply
  -- where process_date <= '${VAR:deal_date}'
) as credit_apply
left join (
  select
  application_no             as apply_no,
  cast(sum(loan_init_principal) as decimal(15,6))   as accu_used_amt
  from dwb.dwb_loan
  -- where d_date = '${VAR:deal_date}'
  group by application_no
) as loan
on credit_apply.apply_no = loan.apply_no
limit 10;



-- dm.dm_watch_asset_pay_flow
-- impala 执行
select
  dwb_order.due_bill_no                         as bill_no,
  case dwb_order.command_type
  when 'SPA' then '单笔代付' when 'SDB' then '单笔代扣'
  when 'BDB' then '批量代扣' when 'BDA' then '批量代付'
  else '其他' end                               as trade_type,
  bank_card_info.pay_channel                    as trade_channel,
  dwb_order.order_id                            as order_no,
  dwb_order.business_date                       as trade_time,
  cast(dwb_order.txn_amt as decimal(15,4))      as order_amount,
  case dwb_order.order_status
  when 'C' then '已提交'     when 'P' then '待提交'     when 'Q' then '待审批'     when 'W' then '处理中'
  when 'S' then '已完成'     when 'V' then '已失效'     when 'E' then '失败'       when 'T' then '超时'
  when 'R' then '已重提'     when 'G' then '拆分处理中' when 'D' then '拆分已完成' when 'B' then '撤销'
  when 'X' then '已受理待入账' else '其他' end  as trade_status
  -- ,'${VAR:deal_date}'                           as d_date
from (
  select due_bill_no,order_id,command_type,business_date,txn_amt,order_status,bank_acct
  from (
    select
      due_bill_no,
      order_id,
      command_type,
      business_date,
      txn_amt,
      order_status,
      bank_acct,
      row_number() over(partition by order_id order by business_date desc) as od
    from dwb.dwb_order
    -- where d_date = '${VAR:deal_date}'
  ) as tmp
  where od = 1
) as dwb_order
left join (
  select card_id,due_bill_no,pay_channel
  from dwb.dwb_bank_card_info
  -- where d_date = '${VAR:deal_date}'
) as bank_card_info
on dwb_order.due_bill_no = bank_card_info.due_bill_no and dwb_order.bank_acct = bank_card_info.card_id
limit 10
;


select distinct trade_channel from dm.dm_watch_asset_pay_flow limit 10;

select distinct pay_channel from dwb.dwb_bank_card_info limit 10;

select * from dwb.dwb_bank_card_info where pay_channel = 2 limit 10;



-- impala::dm.dm_watch_bill_snapshot
select
from (
  select * from dwb.dwb_loan
  -- where d_date = '${VAR:deal_date}'
) as dwb_loan
left join (
  select * from dm.dm_watch_bill_snapshot
  -- where d_date = '${VAR:deal_date}'
) as watch_bill_snapshot on dwb_loan.due_bill_no = watch_bill_snapshot.bill_no






-- impala::dm.dm_watch_contact_person
dwb.dwb_customer_linkman_info
dwb.dwb_loan

-- impala::dm.dm_watch_credit_detail
dwb.dwb_credit_apply
dwb.dwb_credit_result

-- impala::dm.dm_watch_credit_info
dwb.dwb_credit_apply
dwb.dwb_loan_apply
dwb.dwb_risk_control_result
dwb.dwb_loan
dwb.dwb_repay_schedule

-- impala::dm.dm_watch_credit_results
dwb.dwb_loan_apply
dwb.dwb_risk_control_result
dwb.dwb_credit_result

-- impala::dm.dm_watch_on_loan_detail
dwb.dwb_loan_apply
dwb.dwb_loan

-- impala::dm.dm_watch_product_summary
dwb.dwb_loan
dwb.dwb_order
dwb.dwb_credit_apply
dwb.dwb_loan_apply

-- impala::dm.dm_watch_repayment_info
dwb.dwb_repay_hst
dwb.dwb_repay_schedule

-- impala::dm.dm_watch_repayment_schedule
dwb.dwb_repay_schedule
dwb.dwb_repay_hst



select distinct rate_type from dm.dm_watch_bill_snapshot limit 10;
select distinct current_risk_control_status from dm.dm_watch_bill_snapshot limit 10; -- yes 写死了 字段未用

select distinct
ecif_no,channel_code,product_code,is_credit_success,failure_msg,approval_time,
approval_amount,apply_amount,apply_time,credit_id,credit_validity
from dm.dm_watch_credit_detail limit 10;

select is_credit_success,count(is_credit_success) from dm.dm_watch_credit_detail group by is_credit_success limit 10;



select distinct settlement_status from dm.dm_watch_repayment_schedule limit 10;




select distinct risk_control_type,risk_control_result from dm.dm_watch_credit_results limit 10;

select distinct bill_status from dm.dm_watch_bill_snapshot limit 10;

select distinct guaranties,carJson,houseJson,humanjson from dwb.dwb_fj_log_detail limit 10;

select distinct guaranties from dwb.dwb_ht_log_detail limit 10;

select * from ods.ecas_customer limit 10;

select * from ods.ccs_customer limit 50;

select from_unixtime(cast(1574409386000/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as tt from ods.ecas_customer limit 10;

select distinct id_type from ods.ecas_customer limit 10;
select distinct cust_lmt_id from ods.ecas_customer limit 10;
select distinct overflow_amt from ods.ecas_customer limit 10;
select distinct gender from ods.ecas_customer limit 10;
select distinct apply_no from ods.ecas_customer limit 10;

select distinct
  gender,bir_date,marital_status,permanent_address,now_address,bank_no,apply_no,city,job_type,province,country
from ods.ecas_customer
where bank_no != '' or bank_no is not null
-- and marital_status is not null
limit 10;


select * from ods.ecas_msg_log limit 10;



select distinct p_type from dwb.dwb_loan_apply;

select distinct d_date from dwb.dwb_bussiness_customer_info limit 5;

select distinct bir_date from dwb.dwb_bussiness_customer_info limit 10;

select distinct bank_no from dwb.dwb_bussiness_customer_info limit 10;

select distinct cutomer_type from dwb.dwb_customer_info limit 10;


select distinct start_date,end_date from dwb.dwb_credit_apply limit 10;
select distinct resp_code from dwb.dwb_credit_apply limit 10;
select distinct resp_msg from dwb.dwb_credit_apply limit 10;


select distinct cutomer_type from dwb.dwb_dd_log_detail limit 10;

select distinct loan_id,due_bill_no from ods.ecas_loan limit 10;

select distinct payment_id from dwb.dwb_repay_hst limit 10;

desc dwb.dwb_dd_log_detail;


select distinct current_overdue_stage from dm.dm_watch_bill_snapshot limit 10;

select * from ods.ecas_bind_card limit 20;


select distinct card_id,cust_id,org from ods.ecas_bind_card limit 20;


select * from ods.ecas_customer limit 20;
select * from ods.ecas_customer where
gender is not null
or bir_date is not null
or marital_status is not null
or permanent_address is not null
or now_address is not null
or bank_no is not null
or apply_no is not null
or city is not null
or job_type is not null
or province is not null
or country is not null limit 20;


-- org 15601  channel 10043、10000
select distinct org,channel from ods.ecas_customer limit 20;

select distinct channel_id,capital_id,product_code,due_bill_no from dwb.dwb_loan limit 20;

select distinct org,cust_id,apply_no,org,capital_type from ods.ecas_loan limit 20;



select count(1) from ods.ecas_bind_card;

select create_time,lst_upd_time from ods.ecas_bind_card limit 20;




select * from ods.ecas_msg_log limit 10;
select * from ods.nms_interface_resp_log limit 10;
select * from ods.t_real_param limit 10;


show create table ods.ecas_msg_log;

show functions like '*timestamp*';
desc function extended unix_timestamp;


select
  deal_date,
  from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') as create_time,
  from_unixtime(cast(update_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') as update_time
from ods.ecas_msg_log limit 50;

-- ecas_msg_log 的 msg_type 值
-- LOAN_RESULT
-- LOAN_APPLY
-- BIND_BANK_CARD_CHANGE
-- CREDIT_APPLY
-- REPAY_RESULT
-- BIND_BANK_CARD
-- CREDIT_CHANGE

-- 未上线
-- ecas_msg_log 瓜子 msg_type = GZ_CREDIT_APPLY,GZ_CREDIT_RESULT
-- ecas_msg_log 乐信 msg_type = WIND_CONTROL_CREDIT
-- 已上线
-- ecas_msg_log 滴滴 msg_type = CREDIT_APPLY,LOAN_APPLY

-- t_real_param 凤金 interface_name = LOAN_INFO_PER_APPLY

-- nms_interface_resp_log 汇通 sta_service_method_name = setupCustCredit


refresh ods.ecas_msg_log;
select distinct msg_type from ods.ecas_msg_log;

select
 *
from ods.ecas_msg_log
where (msg_type = 'CREDIT_APPLY' or msg_type = 'LOAN_APPLY')
or from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') <= '"+partitionDate_str+"'










-- dwb.dwb_dd_log_detail
select
  ecif_no                                                          as ecif_id,
  get_json_object(original_msg,'$.userInfo.name')                  as name,
  get_json_object(original_msg,'$.userInfo.cardNo')                as cardno,
  null                                                             as idtype,
  get_json_object(original_msg,'$.userInfo.phone')                 as phone,
  get_json_object(original_msg,'$.userInfo.telephone')             as telephone,
  get_json_object(original_msg,'$.userInfo.bankCardNo')            as bankcardno,
  get_json_object(original_msg,'$.userInfo.userRole')              as userrole,
  get_json_object(original_msg,'$.userInfo.idCardValidDate')       as idcardvaliddate,
  get_json_object(original_msg,'$.userInfo.address')               as address,
  get_json_object(original_msg,'$.userInfo.ocrInfo')               as ocrinfo,
  get_json_object(original_msg,'$.userInfo.idCardBackInfo')        as idcardbackinfo,
  get_json_object(original_msg,'$.userInfo.imageType')             as imagetype,
  null                                                             as imagestatus,
  null                                                             as imageurl,
  get_json_object(original_msg,'$.userInfo.livingImageInfo')       as livingimageinfo,
  get_json_object(original_msg,'$.userInfo.sftpDir')               as sftpdir,
  null                                                             as sftp,
  get_json_object(original_msg,'$.creditInfo.amount')              as amount,
  get_json_object(original_msg,'$.creditInfo.interestRate')        as interestrate,
  get_json_object(original_msg,'$.creditInfo.interestPenaltyRate') as interestpenaltyrate,
  get_json_object(original_msg,'$.creditInfo.startDate')           as startdate,
  get_json_object(original_msg,'$.creditInfo.endDate')             as enddate,
  get_json_object(original_msg,'$.creditInfo.lockDownEndTime')     as lockdownendtime,
  get_json_object(original_msg,'$.didiRcFeature')                  as didircfeature,
  get_json_object(original_msg,'$.flowNo')                         as flowno,
  get_json_object(original_msg,'$.signType')                       as signtype,
  get_json_object(original_msg,'$.applicationId')                  as applicationid,
  get_json_object(original_msg,'$.creditResultStatus')             as creditresultstatus, -- Yes,No
  get_json_object(original_msg,'$.applySource')                    as applysource,
  deal_date                                                        as deal_date,
  create_time                                                      as create_time,
  update_time                                                      as update_time,
  withdrawcontactinfo                                              as linkman_info,
  'DIDI201908161538'                                               as product_code,
  org                                                              as org
from (
  select original_msg,deal_date,create_time,update_time,org
  from ods.ecas_msg_log
  where msg_type = 'CREDIT_APPLY'
  and original_msg is not null
  and (
    get_json_object(original_msg,'$.applicationId') is null
    or length(get_json_object(original_msg,'$.applicationId')) >= 20
  )
) as msg_log
left join (
  select id_no,ecif_no from ecif_core.ecif_customer
) as ecif_customer
on encrypt_aes(get_json_object(msg_log.original_msg,'$.userInfo.cardNo'),'weshare666') = ecif_customer.id_no
left join (
  select
  get_json_object(original_msg,'$.creditId') as creditid,
  get_json_object(original_msg,'$.withdrawContactInfo') as withdrawcontactinfo
  from ods.ecas_msg_log
  where msg_type = 'LOAN_APPLY'
) as link_info
on get_json_object(msg_log.original_msg,'$.applicationId') = link_info.creditid
limit 10;







-- dwb.dwb_credit_apply
select
  org as org,
  applicationid as apply_id,
  dim_product.channel_id as channel_id,
  -- ecif_id as ecif_id,
  ecif as ecif_id,
  dwb_dd_log_detail.product_code as product_code,
  applicationid as apply_no,
  substring(applicationid,11,8) as apply_date,
  datefmt(substring(applicationid,11,12),'yyyyMMddHHmm','yyyy-MM-dd HH:mm:ss') as apply_time,
  amount as apply_amt,
  if(creditresultstatus = 'Yes','Y','N') as apply_status,
  null as apply_type,
  null as contr_no,
  null as resp_code,
  if(creditresultstatus = 'Yes','Y',creditresultstatus) as resp_msg,
  datefmt(substring(applicationid,11,12),'yyyyMMddHHmm','yyyy-MM-dd HH:mm:ss') as credit_time,
  substring(applicationid,11,8) as process_date,
  startdate as start_date,
  enddate as end_date,
  amount as limit_amt,
  row_number() over(partition by applicationid,dim_product.channel_id,ecif as ecif_id order by cast(substring(applicationid,11,12) as bigint)) as rn,
  'DD' as p_type
from
-- dwb.dwb_dd_log_detail
(select *,'DIDI201908161538' as product_code from dwb.dwb_dd_log_detail) as dwb_dd_log_detail
left join (
  select product_code,channel_id from dim.dim_product
) as dim_product
on dwb_dd_log_detail.product_code = dim_product.product_code
where rn = 1
limit 10;



-- dwb.dwb_credit_result
select
  org,
  credit_result,
  credit_id,
  apply_id,
  ecif_id,
  product_code,
  amount,
  interest_rate,
  interest_penalty_rate,
  start_date,
  end_date,
  lockdownendtime,
  reject_reason,
  reject_code,
  credit_time,
  p_type
from (
  select
    org                                                                                                        as org,
    if(creditresultstatus = 'Yes','1','0')                                                                     as credit_result,
    applicationid                                                                                              as credit_id,
    applicationid                                                                                              as apply_id,
    -- ecif_id                                                                                                    as ecif_id,
    ecif                                                                                                       as ecif_id,
    dwb_dd_log_detail.product_code                                                                             as product_code,
    amount                                                                                                     as amount,
    interestrate                                                                                               as interest_rate,
    interestpenaltyrate                                                                                        as interest_penalty_rate,
    startdate                                                                                                  as start_date,
    enddate                                                                                                    as end_date,
    lockdownendtime                                                                                            as lockdownendtime,
    if(creditresultstatus = 'Yes','',creditresultstatus)                                                       as reject_reason,
    if(creditresultstatus = 'Yes','',"N")                                                                      as reject_code,
    datefmt(substring(applicationid,11,12),'yyyyMMddHHmm','yyyy-MM-dd HH:mm:ss')                               as credit_time,
    row_number() over(partition by applicationid,ecif order by cast(substring(applicationid,11,12) as bigint)) as rn,
    'DD'                                                                                                       as p_type
  from
  -- dwb.dwb_dd_log_detail
  (select *,'DIDI201908161538' as product_code from dwb.dwb_dd_log_detail) as dwb_dd_log_detail
) as tmp
where rn = 1
limit 10;



-- INSERT OVERWRITE TABLE ods_new_s.credit_apply partition(biz_date)
select
  null                                                            as capital_id,           -- '资金方编号'
  null                                                            as channel_id,           -- '渠道方编号'
  null                                                            as project_id,           -- '项目编号'
  product_id                                                      as product_id,           -- '产品编号'
  null                                                            as cust_id,              -- '客户编号（渠道方编号—用户编号）'
  get_json_object(original_msg,'$.userInfo.cardNo')               as user_hash_no,         -- '用户编号'
  ecif_no                                                         as ecif_id,              -- 'ecif_id'
  get_json_object(original_msg,'$.applicationId')                 as apply_id,             -- '授信申请编号'
  get_json_object(original_msg,'$.creditInfo.startDate')          as credit_apply_time,    -- '授信申请时间（yyyy—MM—dd HH:mm:ss）'
  get_json_object(original_msg,'$.creditInfo.amount')             as apply_amount,         -- '申请金额'
  null                                                            as risk_assessment_time, -- '风控评估时间（yyyy—MM—dd HH:mm:ss）'
  null                                                            as risk_type,            -- '风控类型（用信风控、二次风控）'
  get_json_object(original_msg,'$.creditResultStatus')            as resp_code,            -- '授信申请结果'
  get_json_object(original_msg,'$.creditResultMessage')           as resp_msg,             -- '风控结果有效期（yyyy—MM—dd HH:mm:ss）'
  null                                                            as risk_result_validity, -- '授信结果码'
  get_json_object(original_msg,'$.creditInfo.amount')             as credit_amount,        -- '结果描述'
  get_json_object(original_msg,'$.creditInfo.interestRate')       as credit_interest_rate, -- '授信额度'
  null                                                            as risk_level,           -- '授信利率'
  null                                                            as risk_score,           -- '风控等级'
  original_msg                                                    as ori_request,          -- '风控评分'
  null                                                            as ori_response,         -- '原始请求'
  get_json_object(original_msg,'$.creditInfo.endDate')            as credit_expire_date,   -- '原始应答'
  to_date(get_json_object(original_msg,'$.creditInfo.startDate')) as biz_date              -- '授信截止时间（yyyy—MM—dd HH:mm:ss）'
from (
  select original_msg,'DIDI201908161538' as product_id
  from ods.ecas_msg_log
  where msg_type = 'CREDIT_APPLY'
  and original_msg is not null
  and to_date(get_json_object(original_msg,'$.creditInfo.startDate')) = '2020-04-28'
) as msg_log
left join (
  select id_no,ecif_no from ecif_core.ecif_customer
) as ecif_customer
on encrypt_aes(get_json_object(msg_log.original_msg,'$.userInfo.cardNo')) = ecif_customer.id_no;




-- LOAN_RESULT            无身份证号
-- LOAN_APPLY             无身份证号
-- BIND_BANK_CARD_CHANGE  无身份证号
-- CREDIT_APPLY
-- REPAY_RESULT           无身份证号
-- BIND_BANK_CARD         无身份证号
-- CREDIT_CHANGE          无身份证号
-- 提交借款申请接口 {"loanOrderId":"DD0002303620191008182200eafd91","message":"交易失败","status":2}
select original_msg,deal_date,create_time,update_time from ods.ecas_msg_log where msg_type = 'CREDIT_APPLY' limit 10;

select distinct get_json_object(original_msg,'$.creditInfo.lockDownEndTime') as lockdownendtime from ods.ecas_msg_log limit 10;

select distinct
  msg_type,
  get_json_object(original_msg,'$.userInfo.cardNo') as cardno
from ods.ecas_msg_log
where msg_type = 'CREDIT_APPLY'
and get_json_object(original_msg,'$.userInfo.cardNo') is null
limit 10;



select count(1) as cnt from ods.ecas_msg_log;

select
  get_json_object(original_msg,'$.applicationId') as apply_id,
  count(get_json_object(original_msg,'$.applicationId')) as cnt,
  original_msg
from ods.ecas_msg_log
-- where get_json_object(original_msg,'$.applicationId') = 'DD0000303620200429131048b766fc'
group by get_json_object(original_msg,'$.applicationId'),original_msg
having count(get_json_object(original_msg,'$.applicationId')) > 1
limit 10;

select distinct
  get_json_object(original_msg,'$.name') as name,
  get_json_object(original_msg,'$.idNo') as idno,
  get_json_object(original_msg,'$.mobile') as mobile,
  msg_type,
  original_msg
from ods.ecas_msg_log
limit 10;


select
  msg_type,
  get_json_object(original_msg,'$.creditResult')   as creditResult,
  get_json_object(original_msg,'$.creditId')       as creditId,
  get_json_object(original_msg,'$.applicationId')  as applicationId,
  get_json_object(original_msg,'$.creditInfo')     as creditInfo,
  get_json_object(original_msg,'$.refusalReasons') as refusalReasons,
  get_json_object(original_msg,'$.extendsInfo')    as extendsInfo
from ods.ecas_msg_log
where get_json_object(original_msg,'$.refusalReasons') is not null
limit 20;


select get_json_object(original_msg,'$.extendsInfo') as extendsinfo from ods.ecas_msg_log where get_json_object(original_msg,'$.extendsInfo') is not null limit 10;

select
  get_json_object(original_msg,'$.name') as name,
  get_json_object(original_msg,'$.userInfo.name') as user_name
from ods.ecas_msg_log limit 100;

select
  get_json_object(original_msg,'$.creditInfo.startDate') as startDate,
  get_json_object(original_msg,'$.creditInfo.endDate') as endDate
from ods.ecas_msg_log
where msg_type = 'CREDIT_APPLY'
-- and get_json_object(original_msg,'$.creditInfo.startDate') is null
and get_json_object(original_msg,'$.creditInfo.endDate') is null
limit 100;


select distinct
  get_json_object(original_msg,'$.creditInfo.lockDownEndTime') as lockdownendtime
from ods.ecas_msg_log
limit 10;


select distinct
  get_json_object(original_msg,'$.creditInfo') as creditinfo
from ods.ecas_msg_log
where get_json_object(original_msg,'$.applicationId') = 'DD00003036202002251634942fe94c'
limit 10;


select distinct lockdown_end_time from dwb.dwb_credit_result limit 10;

select distinct * from dwb.dwb_credit_result where lockdown_end_time = 0 limit 10;




desc dwb.dwb_credit_apply;

select distinct apply_type,contr_no,resp_code from dwb.dwb_credit_apply;

select apply_time from dwb.dwb_credit_apply limit 10;


desc dim.dim_account_info;
desc dim.dim_bank_card_info;
desc dim.dim_budget_plan;
desc dim.dim_card_area;
desc dim.dim_channel;
select * from dim.dim_channel;
desc dim.dim_customer_info;
desc dim.dim_customer_info_bank;
desc dim.dim_customer_linkman_info;
desc dim.dim_customer_travel;
desc dim.dim_guarantees_info;
desc dim.dim_natural_days;
desc dim.dim_phone_area;
desc dim.dim_product;
select * from dim.dim_product;
desc dim.dim_scene_channel;
select * from dim.dim_scene_channel;
desc dim.dim_scene_product;
select * from dim.dim_scene_product;


select
  to_date(get_json_object(original_msg,'$.creditInfo.startDate')) as biz_date,
  count(1) as cnt
from ods.ecas_msg_log
where msg_type = 'CREDIT_APPLY'
group by to_date(get_json_object(original_msg,'$.creditInfo.startDate'))
order by biz_date
;













-- ecas_msg_log 的 msg_type 值
-- CREDIT_APPLY
-- CREDIT_CHANGE
-- LOAN_APPLY
-- LOAN_RESULT
-- BIND_BANK_CARD_CHANGE
-- BIND_BANK_CARD
-- REPAY_RESULT

-- 未上线
-- ecas_msg_log 瓜子 msg_type = GZ_CREDIT_APPLY,GZ_CREDIT_RESULT
-- ecas_msg_log 乐信 msg_type = WIND_CONTROL_CREDIT
-- 已上线
-- ecas_msg_log 滴滴 msg_type = CREDIT_APPLY,LOAN_APPLY

-- t_real_param 凤金 interface_name = LOAN_INFO_PER_APPLY

-- nms_interface_resp_log 汇通 sta_service_method_name = setupCustCredit
select
  original_msg
from ods.ecas_msg_log
where msg_type = 'CREDIT_CHANGE'
limit 10;

select
  deal_date,
  get_json_object(original_msg,'$.loanOrderId') as loan_order_id,
  datefmt(create_time,'',''),
  create_time
  -- from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') as create_time,
  -- from_unixtime(cast(update_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') as update_time
from ods.ecas_msg_log
where msg_type = 'LOAN_RESULT'
limit 10;


select distinct
  get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\"\{','\{'),'\}\"','\}'),'$.withdrawContactInfo.jobType') as jobtype
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
limit 10;

select
  get_json_object(original_msg,'$.loanOrderId') as loan_order_id,
  count(get_json_object(original_msg,'$.loanOrderId')) as cnt
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
group by get_json_object(original_msg,'$.loanOrderId')
having count(get_json_object(original_msg,'$.loanOrderId')) > 1
limit 10;


select
  get_json_object(original_msg,'$.creditId') as creditid,
  get_json_object(original_msg,'$.loanOrderId') as loanorderid,
  get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\"\{','\{'),'\}\"','\}'),'$.withdrawContactInfo') as withdrawcontactinfo
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
and get_json_object(original_msg,'$.loanOrderId') = 'DD0002303620200217080200e2783e'
limit 10;






-- 凤金
select
  id as id,
  agency_id as agency_id,
  project_id as project_id,
  partition_key as partition_key,
  create_time as create_time,
  update_time as update_time,
  15601 as org,
  get_json_object(requst_data,'$.product_no') as product_no,
  get_json_object(requst_data,'$.request_no') as request_no,
  get_json_object(requst_data,'$.loan_apply_use') as loan_apply_use,
  get_json_object(requst_data,'$.loan_rate_type') as loan_rate_type,
  get_json_object(requst_data,'$.currency_type') as currency_type,
  get_json_object(requst_data,'$.contract_no') as contract_no,
  get_json_object(requst_data,'$.contract_amount') as contract_amount,
  get_json_object(requst_data,'$.company_loan_bool') as company_loan_bool,
  get_json_object(requst_data,'$.guaranties') as guaranties,
  get_json_object(requst_data,'$.relational_humans') as relational_humans,
  get_json_object(requst_data,'$.repayment_account') as repayment_account,
  get_json_object(requst_data,'$.loan_account') as loan_account,
  get_json_object(requst_data,'$.car') as car,
  get_json_object(requst_data,'$.borrower.open_id') as open_id,
  get_json_object(requst_data,'$.borrower.name') as name,
  get_json_object(requst_data,'$.borrower.id_type') as id_type,
  get_json_object(requst_data,'$.borrower.id_no') as id_no,
  ecif_no as ecif_id,
  if(get_json_object(requst_data,'$.borrower.sex') is null or length(get_json_object(requst_data,'$.borrower.sex')) = 0,
    sex_idno(get_json_object(requst_data,'$.borrower.id_no')),
    case get_json_object(requst_data,'$.borrower.sex')
    when 'F' then '女' when 'M' then '男'
    else get_json_object(requst_data,'$.borrower.sex')
    end
  ) as sex,
  if(get_json_object(requst_data,'$.borrower.age') is null or length(get_json_object(requst_data,'$.borrower.age')) = 0,
    year(get_json_object(requst_data,'$.schedule_base.loan_date')) - cast(datefmt(substring(get_json_object(requst_data,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy') as int),
    cast(get_json_object(requst_data,'$.borrower.age') as int)
  ) as age,
  get_json_object(requst_data,'$.borrower.mobile_phone') as mobile_phone,
  get_json_object(requst_data,'$.borrower.province') as province,
  get_json_object(requst_data,'$.borrower.city') as city,
  get_json_object(requst_data,'$.borrower.area') as area,
  get_json_object(requst_data,'$.borrower.address') as address,
  get_json_object(requst_data,'$.borrower.marital_status') as marital_status,
  get_json_object(requst_data,'$.borrower.education') as education,
  get_json_object(requst_data,'$.borrower.industry') as industry,

  get_json_object(requst_data,'$.borrower.annual_income') as annual_income,
  get_json_object(requst_data,'$.borrower.have_house') as have_house,
  get_json_object(requst_data,'$.borrower.housing_area') as housing_area,
  get_json_object(requst_data,'$.borrower.housing_value') as housing_value,
  get_json_object(requst_data,'$.borrower.family_worth') as family_worth,

  get_json_object(requst_data,'$.borrower.front_url') as front_url,
  get_json_object(requst_data,'$.borrower.back_url') as back_url,

  get_json_object(requst_data,'$.borrower.private_owners') as private_owners,
  get_json_object(requst_data,'$.borrower.income_m1') as income_m1,
  get_json_object(requst_data,'$.borrower.income_m2') as income_m2,
  get_json_object(requst_data,'$.borrower.income_m3') as income_m3,
  get_json_object(requst_data,'$.borrower.income_m4') as income_m4,
  get_json_object(requst_data,'$.borrower.social_credit_code') as social_credit_code,
  get_json_object(requst_data,'$.borrower.company_name') as company_name,

  get_json_object(requst_data,'$.borrower.industry') as industry,
  get_json_object(requst_data,'$.borrower.legal_person_name') as legal_person_name,
  get_json_object(requst_data,'$.borrower.legal_person_phone') as legal_person_phone,
  get_json_object(requst_data,'$.borrower.phone') as phone,
  get_json_object(requst_data,'$.borrower.operate_years') as operate_years,
  get_json_object(requst_data,'$.schedule_base.repay_type') as repay_type,
  get_json_object(requst_data,'$.schedule_base.repay_frequency') as repay_frequency,
  get_json_object(requst_data,'$.schedule_base.terms') as terms,
  get_json_object(requst_data,'$.schedule_base.deduction_date') as deduction_date,
  get_json_object(requst_data,'$.schedule_base.loan_rate') as loan_rate,
  get_json_object(requst_data,'$.schedule_base.year_rate_base') as year_rate_base,
  get_json_object(requst_data,'$.schedule_base.loan_date') as loan_date,
  get_json_object(requst_data,'$.schedule_base.loan_end_date') as loan_end_date,
  get_json_object(resp_data,'$.apply_request_no') as apply_request_no,
  get_json_object(resp_data,'$.acct_setup_ind') as acct_setup_ind
from (
  select
    id,
    agency_id,
    project_id,
    partition_key,
    create_time,
    update_time,
    requst_data,
    resp_data
  from ods.t_real_param
  where interface_name = 'LOAN_INFO_PER_APPLY'
  and agency_id = '0004'
  and requst_data is not null
) as t_real_param
left join (
  select id_no,ecif_no from ecif_core.ecif_customer_hive
) as ecif_customer
on encrypt_aes(get_json_object(t_real_param.requst_data,'$.borrower.id_no')) = ecif_customer.id_no
limit 10;




select
  id,
  agency_id,
  project_id,
  partition_key,
  create_time,
  update_time,
  requst_data,
  resp_data
from ods.t_real_param
where interface_name = 'LOAN_INFO_PER_APPLY'
and agency_id = '0004'
and requst_data is not null
limit 10;


-- LOAN_INFO_PER_APPLY
-- LOAN_SCHEDULE_CALC
-- LOAN_REPAY
-- REPAY_PLAN_QUERY
select distinct interface_name from ods.t_real_param;


select
  distinct
  -- get_json_object(requst_data,'$.borrower.id_no') as id_no,
  -- substring(get_json_object(requst_data,'$.borrower.id_no'),17,1) as sex
  -- length(get_json_object(requst_data,'$.borrower.id_no')) as len
  -- case get_json_object(requst_data,'$.borrower.sex') when 'F' then '女' when 'M' then '男' else get_json_object(requst_data,'$.borrower.sex') end as gender,
  -- sex_idno(get_json_object(requst_data,'$.borrower.id_no')) as sex
  -- if(case get_json_object(requst_data,'$.borrower.sex') when 'F' then '女' when 'M' then '男' else get_json_object(requst_data,'$.borrower.sex') end != sex_idno(get_json_object(requst_data,'$.borrower.id_no')),'false','true') as tf

  get_json_object(requst_data,'$.borrower.age') as age,
  get_json_object(requst_data,'$.schedule_base.loan_date') as loan_date,
  datefmt(substring(get_json_object(requst_data,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy-MM-dd') as birthday,
  year(get_json_object(requst_data,'$.schedule_base.loan_date')) - cast(datefmt(substring(get_json_object(requst_data,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy') as int) as age1,
  if(cast(get_json_object(requst_data,'$.borrower.age') as int) = year(get_json_object(requst_data,'$.schedule_base.loan_date')) - cast(datefmt(substring(get_json_object(requst_data,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy') as int),'true','false') as a
from ods.t_real_param
where interface_name = 'LOAN_INFO_PER_APPLY'
and agency_id = '0004'
and requst_data is not null
-- and (get_json_object(requst_data,'$.borrower.age') is null or length(get_json_object(requst_data,'$.borrower.age')) = 0)
and cast(get_json_object(requst_data,'$.borrower.age') as int) != year(get_json_object(requst_data,'$.schedule_base.loan_date')) - cast(datefmt(substring(get_json_object(requst_data,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy') as int)
-- limit 10
;


select
  partition_key,
  create_time,
  update_time,
  concat(
        substring(get_json_object(requst_data,'$.schedule_base.loan_date'),1,4),
        substring(get_json_object(requst_data,'$.schedule_base.loan_date'),6,2),
        substring(get_json_object(requst_data,'$.schedule_base.loan_date'),9,2)
        ) as loan_date_t,
  get_json_object(requst_data,'$.schedule_base.loan_date') as loan_date,
from ods.t_real_param
where interface_name = 'LOAN_INFO_PER_APPLY'
and agency_id = '0004'
and requst_data is not null
and cast(partition_key as string) != concat(
                            substring(get_json_object(requst_data,'$.schedule_base.loan_date'),1,4),
                            substring(get_json_object(requst_data,'$.schedule_base.loan_date'),6,2),
                            substring(get_json_object(requst_data,'$.schedule_base.loan_date'),9,2)
                            )
limit 10;


select
  -- distinct
  -- count(get_json_object(requst_data,'$.schedule_base.loan_date')) as cnt_loan_date,
  count(1) as cnt
from ods.t_real_param
where interface_name = 'LOAN_INFO_PER_APPLY'
and agency_id = '0004'
;



select
  requst_data
from ods.t_real_param
where interface_name = 'LOAN_INFO_PER_APPLY'
and agency_id = '0004'
limit 1
;









+-------------------------+--------------+-------------------------------------------------------------+
| name                    | type         | comment                                                     |
+-------------------------+--------------+-------------------------------------------------------------+
| id                      | string       | 接口响应日志ID : ///@UUIDSeq                                |
| req_log_id              | string       | 请求日志ID : 请求日志ID                                     |
| sta_service_method_name | string       | 标准请求模板编号                                            |
| standard_req_msg        | string       | 标准请求报文                                                |
| standard_resp_msg       | string       | 标准响应报文 : 标准响应报文                                 |
| resp_msg                | string       | 响应报文 : 响应报文                                         |
| resp_code               | string       | 响应码 : 响应码                                             |
| resp_desc               | string       | 响应描述 : 响应描述                                         |
| deal_date               | string       | 请求处理时间 : 请求处理时间                                 |
| status                  | string       | 请求状态, SUC:成功,FAIL:失败 : 请求状态, SUC:成功,FAIL:失败 |
| org                     | string       | 机构号 : 机构号                                             |
| create_time             | bigint       | 创建时间 : 创建时间                                         |
| update_time             | bigint       | 更新时间 : 更新时间                                         |
| jpa_version             | decimal(8,0) | 乐观锁版本号 : 乐观锁版本号                                 |
+-------------------------+--------------+-------------------------------------------------------------+
desc ods.nms_interface_resp_log;


-- batchLoanApplyQuery
-- setupCustCredit
-- queryCapRepaySchedule
-- refundConfirm
-- preCheckForSignature
-- payNotify
-- loanApply
-- batchRedemptionQuery
-- collectionRepay
-- amountAdj
-- queryCollectionRepayResult
-- offlineRepayQuery
-- offlineRepay
-- checkCapitalScale
-- onlineRepay
-- updateBindCard
-- loanApplyCancel
select distinct sta_service_method_name from ods.nms_interface_resp_log;


select * from ods.nms_interface_resp_log where sta_service_method_name = 'setupCustCredit' limit 10;



select
  case get_json_object(standard_resp_msg,'$.acct_setup_ind')
  when 'Y' then '成功' when 'N' then '失败'
  else get_json_object(standard_resp_msg,'$.acct_setup_ind')
  end as acct_setup_ind,
  get_json_object(standard_resp_msg,'$.cust_no') as cust_no,
  get_json_object(standard_resp_msg,'$.reject_msg') as reject_msg,

  get_json_object(standard_req_msg,'$.pre_apply_no') as pre_apply_no,
  get_json_object(standard_req_msg,'$.apply_no') as apply_no,
  get_json_object(standard_req_msg,'$.company_loan_bool') as company_loan_bool,
  get_json_object(standard_req_msg,'$.relational_humans') as relational_humans,
  get_json_object(standard_req_msg,'$.guaranties') as guaranties,

  -- get_json_object(standard_req_msg,'$.car') as car -- car 在 guaranties 中

  get_json_object(standard_req_msg,'$.product.product_no') as product_no,
  case get_json_object(standard_req_msg,'$.product.currency_type')
  when 'RMB' then '人民币'
  else get_json_object(standard_req_msg,'$.product.currency_type')
  end                                                          as currency_type,
  get_json_object(standard_req_msg,'$.product.currency_amt') as currency_amt,
  get_json_object(standard_req_msg,'$.product.loan_amt') as loan_amt,
  get_json_object(standard_req_msg,'$.product.loan_terms') as loan_terms,
  case get_json_object(standard_req_msg,'$.product.repay_type')
  when 'RT01' then '等额本息'
  when 'RT02' then '等额本金'
  when 'RT03' then '等本等息'
  when 'RT04' then '一次还本付息'
  when 'RT05' then '按月付息-到期一次性还本'
  when 'RT06' then '循环授信-随借随还'
  when 'RT07' then '循环授信-随借随还'
  when 'RT08' then '循环授信-随借随还'
  else get_json_object(standard_req_msg,'$.product.repay_type')
  end                                                          as repay_type,
  case get_json_object(standard_req_msg,'$.product.loan_rate_type')
  when 'LRT01' then '固定利率'
  else get_json_object(standard_req_msg,'$.product.loan_rate_type')
  end                                                          as loan_rate_type,
  case get_json_object(standard_req_msg,'$.product.agreement_rate_ind')
  when 'Y' then '是' when 'N' then '否'
  else get_json_object(standard_req_msg,'$.product.agreement_rate_ind')
  end as agreement_rate_ind,
  get_json_object(standard_req_msg,'$.product.loan_rate') as loan_rate,
  get_json_object(standard_req_msg,'$.product.loan_fee_rate') as loan_fee_rate,
  get_json_object(standard_req_msg,'$.product.loan_svc_fee_rate') as loan_svc_fee_rate,
  get_json_object(standard_req_msg,'$.product.loan_penalty_rate') as loan_penalty_rate,
  get_json_object(standard_req_msg,'$.product.guarantee_type') as guarantee_type,
  case get_json_object(standard_req_msg,'$.product.loan_apply_use')
  when 'LAU99' then '其他类消费'
  when 'LAU01' then '购车'
  when 'LAU02' then '购房'
  when 'LAU03' then '医疗'
  when 'LAU04' then '国内教育'
  when 'LAU05' then '出境留学'
  when 'LAU06' then '装修'
  when 'LAU07' then '婚庆'
  when 'LAU08' then '旅游'
  when 'LAU09' then '租赁'
  when 'LAU10' then '美容'
  when 'LAU11' then '家具'
  when 'LAU12' then '生活用品'
  when 'LAU13' then '家用电器'
  when 'LAU14' then '数码产品'
  else get_json_object(standard_req_msg,'$.product.loan_apply_use')
  end                                                          as loan_apply_use,
  ecif_no                                                           as ecif_id,
  get_json_object(standard_req_msg,'$.borrower.open_id')            as open_id,
  case get_json_object(standard_req_msg,'$.borrower.id_type')
  when 'I' then '身份证'
  when 'T' then '台胞证'
  when 'S' then '军官证/士兵证'
  when 'P' then '护照'
  when 'L' then '营业执照'
  when 'O' then '其他有效证件'
  when 'R' then '户口簿'
  when 'H' then '港澳居民来往内地通行证'
  when 'W' then '台湾同胞来往内地通行证'
  when 'F' then '外国人居留证'
  when 'C' then '警官证'
  when 'B' then '外国护照'
  else get_json_object(standard_req_msg,'$.borrower.id_type')
  end                                                               as id_type,
  get_json_object(standard_req_msg,'$.borrower.id_no')              as id_no,
  get_json_object(standard_req_msg,'$.borrower.name')               as name,
  get_json_object(standard_req_msg,'$.borrower.mobile_phone')       as mobile_phone,
  get_json_object(standard_req_msg,'$.borrower.province')           as province,
  get_json_object(standard_req_msg,'$.borrower.city')               as city,
  get_json_object(standard_req_msg,'$.borrower.area')               as area,
  get_json_object(standard_req_msg,'$.borrower.address')            as address,
  case get_json_object(standard_req_msg,'$.borrower.marital_status')
  when 'C' then '已婚'
  when 'S' then '未婚'
  when 'O' then '其他'
  when 'D' then '离异'
  when 'P' then '丧偶'
  else get_json_object(standard_req_msg,'$.borrower.marital_status')
  end                                                               as marital_status,
  case get_json_object(standard_req_msg,'$.borrower.education')
  when 'A' then '博士及以上'
  when 'B' then '硕士'
  when 'C' then '大学本科'
  when 'D' then '大学专科/专科学校'
  when 'E' then '高中/中专/技校'
  when 'F' then '初中'
  when 'G' then '初中以下'
  else get_json_object(standard_req_msg,'$.borrower.education')
  end                                                               as education,
  case get_json_object(standard_req_msg,'$.borrower.industry')
  when 'A' then '农、林、牧、渔业'
  when 'B' then '采掘业'
  when 'C' then '制造业'
  when 'D' then '电力、燃气及水的生产和供应业'
  when 'E' then '建筑业'
  when 'F' then '交通运输、仓储和邮政业'
  when 'G' then '信息传输、计算机服务和软件业'
  when 'H' then '批发和零售业'
  when 'I' then '住宿和餐饮业'
  when 'J' then '金融业'
  when 'K' then '房地产业'
  when 'L' then '租赁和商务服务业'
  when 'M' then '科学研究、技术服务业和地质勘察业'
  when 'N' then '水利、环境和公共设施管理业'
  when 'O' then '居民服务和其他服务业'
  when 'P' then '教育'
  when 'Q' then '卫生、社会保障和社会福利业'
  when 'R' then '文化、体育和娱乐业'
  when 'S' then '公共管理和社会组织'
  when 'T' then '国际组织'
  when 'Z' then '其他'
  when 'NIL' then '空'
  else get_json_object(standard_req_msg,'$.borrower.industry')
  end                                                               as industry,
  get_json_object(standard_req_msg,'$.borrower.annual_income_max')  as annual_income_max,
  get_json_object(standard_req_msg,'$.borrower.annual_income_min')  as annual_income_min,
  if(length(get_json_object(standard_req_msg,'$.borrower.have_house')) = 0,null,get_json_object(standard_req_msg,'$.borrower.have_house'))         as have_house,
  get_json_object(standard_req_msg,'$.borrower.housing_area')       as housing_area,
  get_json_object(standard_req_msg,'$.borrower.housing_value')      as housing_value,
  if(length(get_json_object(standard_req_msg,'$.borrower.drivr_licen_no')) = 0,null,get_json_object(standard_req_msg,'$.borrower.drivr_licen_no'))     as drivr_licen_no,
  get_json_object(standard_req_msg,'$.borrower.driving_expr')       as driving_expr,
  if(get_json_object(standard_req_msg,'$.borrower.sex') is null or length(get_json_object(standard_req_msg,'$.borrower.sex')) = 0,
    sex_idno(get_json_object(standard_req_msg,'$.borrower.id_no')),
    case get_json_object(standard_req_msg,'$.borrower.sex')
    when 'M' then '男' when 'F' then '女'
    else get_json_object(standard_req_msg,'$.borrower.sex')
    end
  ) as sex,
  get_json_object(standard_req_msg,'$.borrower.age')                as age,
  if(length(get_json_object(standard_req_msg,'$.company.social_credit_code')) = 0,null,get_json_object(standard_req_msg,'$.company.social_credit_code')) as social_credit_code,
  if(length(get_json_object(standard_req_msg,'$.company.company_name')) = 0,null,get_json_object(standard_req_msg,'$.company.company_name')) as company_name,
  if(length(get_json_object(standard_req_msg,'$.company.industry')) = 0,null,get_json_object(standard_req_msg,'$.company.industry')) as industry,
  if(length(get_json_object(standard_req_msg,'$.company.province')) = 0,null,get_json_object(standard_req_msg,'$.company.province')) as province,
  if(length(get_json_object(standard_req_msg,'$.company.city')) = 0,null,get_json_object(standard_req_msg,'$.company.city')) as city,
  if(length(get_json_object(standard_req_msg,'$.company.address')) = 0,null,get_json_object(standard_req_msg,'$.company.address')) as address,
  if(length(get_json_object(standard_req_msg,'$.company.legal_person_name')) = 0,null,get_json_object(standard_req_msg,'$.company.legal_person_name')) as legal_person_name,
  if(length(get_json_object(standard_req_msg,'$.company.id_type')) = 0,null,get_json_object(standard_req_msg,'$.company.id_type')) as id_type,
  if(length(get_json_object(standard_req_msg,'$.company.id_no')) = 0,null,get_json_object(standard_req_msg,'$.company.id_no')) as id_no,
  if(length(get_json_object(standard_req_msg,'$.company.legal_person_phone')) = 0,null,get_json_object(standard_req_msg,'$.company.legal_person_phone')) as legal_person_phone,
  if(length(get_json_object(standard_req_msg,'$.company.phone')) = 0,null,get_json_object(standard_req_msg,'$.company.phone')) as phone,
  if(length(get_json_object(standard_req_msg,'$.company.operate_years')) = 0,0,cast(get_json_object(standard_req_msg,'$.company.operate_years') as int)) as operate_years,
  case get_json_object(standard_req_msg,'$.loan_account.account_type')
  when 'ERSONAL' then '个人账户' when 'BUSINESS' then '对公账户'
  else get_json_object(standard_req_msg,'$.loan_account.account_type')
  end                                                             as loan_account_account_type,
  get_json_object(standard_req_msg,'$.loan_account.account_num')  as loan_account_account_num,
  get_json_object(standard_req_msg,'$.loan_account.account_name') as loan_account_account_name,
  case get_json_object(standard_req_msg,'$.loan_account.bank_name')
  when 'B0100' then '邮储银行'
  when 'B0102' then '中国工商银行'
  when 'B0103' then '中国农业银行'
  when 'B0104' then '中国建设银行'
  when 'B0105' then '交通银行'
  when 'B0301' then '中信银行'
  when 'B0302' then '中国光大银行'
  when 'B0303' then '中国民生银行'
  when 'B0305' then '广东发展银行'
  when 'B0306' then '深发展银行'
  when 'B0307' then '招商银行'
  when 'B0308' then '兴业银行'
  when 'B0410' then '中国平安银行'
  when 'B6440' then '徽商银行'
  when 'B0411' then '中国银行'
  else get_json_object(standard_req_msg,'$.loan_account.bank_name')
  end as loan_account_bank_name,
  if(length(get_json_object(standard_req_msg,'$.loan_account.branch_name')) = 0,null,get_json_object(standard_req_msg,'$.loan_account.branch_name'))  as loan_account_branch_name,
  get_json_object(standard_req_msg,'$.loan_account.mobile_phone') as loan_account_mobile_phone
from (
  select
    id,
    deal_date,
    create_time,
    update_time,
    req_log_id,
    org,
    standard_req_msg,
    standard_resp_msg,
    status
  from ods.nms_interface_resp_log
  where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
) as resp_log
left join (
  select id_no,ecif_no from ecif_core.ecif_customer_hive
) as ecif_customer
on encrypt_aes(get_json_object(resp_log.standard_req_msg,'$.borrower.id_no')) = ecif_customer.id_no
limit 10;









-- 联系人信息表
select
  linkman_info.capital_id,
  linkman_info.channel_id,
  linkman_info.project_id,
  linkman_info.product_id,
  linkman_info.cust_id,
  linkman_info.user_hash_no,
  linkman_info.ecif_id,
  linkman_info.due_bill_no,
  linkman_info.linkman_id,
  linkman_info.relational_type,
  linkman_info.relationship,
  linkman_info.relation_idcard_type,
  linkman_info.relation_idcard_no,
  linkman_info.relation_birthday,
  linkman_info.relation_name,
  linkman_info.relation_sex,
  linkman_info.relation_mobile,
  linkman_info.relation_address,
  linkman_info.relation_province,
  linkman_info.relation_city,
  linkman_info.relation_county,
  linkman_info.corp_type,
  linkman_info.corp_name,
  linkman_info.corp_teleph_nbr,
  linkman_info.corp_fax,
  linkman_info.corp_position,
  linkman_info.effective_time,
  cast(if(to_date(linkman_info.expire_time) > '2020-05-06' and msg_log.linkman_id is not null,msg_log.create_time,linkman_info.expire_time) as timestamp) as expire_time
from ods_new_s.linkman_info
left join (
  select distinct
    cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss.SSS') as timestamp) as create_time,
    concat(
      get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'$.idNo'),
      '_',
      get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'$.withdrawContactInfo.emergencyContactMobile')
    ) as linkman_id
  from ods.ecas_msg_log
  where msg_type = 'LOAN_APPLY'
  and original_msg is not null
  and deal_date = '2020-05-06'
) as msg_log
on linkman_info.linkman_id = msg_log.linkman_id
union all
select distinct
  null                                              as capital_id,
  null                                              as channel_id,
  null                                              as project_id,
  product_id                                        as product_id,
  null                                              as cust_id,
  get_json_object(original_msg,'$.idNo')            as user_hash_no,
  ecif_no                                           as ecif_id,
  get_json_object(original_msg,'$.loanOrderId')     as due_bill_no,
  concat(get_json_object(original_msg,'$.idNo'),'_',get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactMobile')) as linkman_id,
  null                                              as relational_type,
  case get_json_object(original_msg,'$.withdrawContactInfo.relationship')
  when '1' then '父母'
  when '2' then '配偶'
  when '3' then '子女'
  when '4' then '兄弟姐妹'
  else get_json_object(original_msg,'$.withdrawContactInfo.relationship')
  end                                                                          as relationship,
  null                                              as relation_idcard_type,
  null                                              as relation_idcard_no,
  null                                              as relation_birthday,
  get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactName')   as relation_name,
  null                                              as relation_sex,
  get_json_object(original_msg,'$.withdrawContactInfo.emergencyContactMobile') as relation_mobile,
  null                                              as relation_address,
  null                                              as relation_province,
  null                                              as relation_city,
  null                                              as relation_county,
  null                                              as corp_type,
  null                                              as corp_name,
  null                                              as corp_teleph_nbr,
  null                                              as corp_fax,
  null                                              as corp_position,
  cast(create_time as timestamp)                    as effective_time,
  cast('3000-12-31 00:00:00' as timestamp)          as expire_time
from (
  select
    'DIDI201908161538' as product_id,
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss.SSS') as create_time,
    regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}') as original_msg
  from ods.ecas_msg_log
  where msg_type = 'LOAN_APPLY'
  and original_msg is not null
  and deal_date = '2020-05-06'
) as msg_log
left join (
  select id_no,ecif_no from ecif_core.ecif_customer_hive
) as ecif_customer
on encrypt_aes(get_json_object(original_msg,'$.idNo')) = ecif_customer.id_no
union all
select
  linkman_info.capital_id,
  linkman_info.channel_id,
  linkman_info.project_id,
  linkman_info.product_id,
  linkman_info.cust_id,
  linkman_info.user_hash_no,
  linkman_info.ecif_id,
  linkman_info.due_bill_no,
  linkman_info.linkman_id,
  linkman_info.relational_type,
  linkman_info.relationship,
  linkman_info.relation_idcard_type,
  linkman_info.relation_idcard_no,
  linkman_info.relation_birthday,
  linkman_info.relation_name,
  linkman_info.relation_sex,
  linkman_info.relation_mobile,
  linkman_info.relation_address,
  linkman_info.relation_province,
  linkman_info.relation_city,
  linkman_info.relation_county,
  linkman_info.corp_type,
  linkman_info.corp_name,
  linkman_info.corp_teleph_nbr,
  linkman_info.corp_fax,
  linkman_info.corp_position,
  linkman_info.effective_time,
  cast(if(to_date(linkman_info.expire_time) > '2020-05-06' and t_real_param.linkman_id is not null,t_real_param.create_time,linkman_info.expire_time) as timestamp) as expire_time
from ods_new_s.linkman_info
left join (
  select distinct
    create_time,
    concat(get_json_object(requst_data,'$.borrower.id_no'),'_',relational_humans['mobile_phone'])  as linkman_id
  from ods.t_real_param
  lateral view explode(json_array_to_array(get_json_object(requst_data,'$.relational_humans'))) humans as relational_humans
  where interface_name = 'LOAN_INFO_PER_APPLY'
  and agency_id = '0004'
  and requst_data is not null
  and to_date(create_time) = '2020-03-03'
) as t_real_param
on linkman_info.linkman_id = t_real_param.linkman_id
union all
select
  null                                                              as capital_id,
  null                                                              as channel_id,
  null                                                              as project_id,
  product_id                                                        as product_id,
  null                                                              as cust_id,
  t_real_param.id_no                                                as user_hash_no,
  ecif_no                                                           as ecif_id,
  t_real_param.request_no                                           as due_bill_no,
  concat(t_real_param.id_no,'_',relational_humans['mobile_phone'])  as linkman_id,
  case relational_humans['relational_human_type']
  when 'RHT01' then '借款人联系人'
  when 'RHT02' then '共同借款人'
  when 'RHT03' then '抵押人'
  when 'RHT04' then '抵押人家庭成员信息'
  when 'RHT05' then '保证人-个人信用保证'
  else relational_humans['relational_human_type']
  end                                                               as relational_type,
  case relational_humans['relationship']
  when 'C' then '配偶'
  when 'F' then '父亲'
  when 'M' then '母亲'
  when 'B' then '兄弟'
  when 'S' then '姐妹'
  when 'L' then '亲属'
  when 'W' then '同事'
  when 'D' then '父母'
  when 'H' then '子女'
  when 'X' then '兄弟姐妹'
  when 'T' then '同学'
  when 'Y' then '朋友'
  when 'O' then '其他'
  else relational_humans['relationship']
  end                                                               as relationship,
  null                                                              as relation_idcard_type,
  null                                                              as relation_idcard_no,
  null                                                              as relation_birthday,
  relational_humans['name']                                         as relation_name,
  null                                                              as relation_sex,
  relational_humans['mobile_phone']                                 as relation_mobile,
  null                                                              as relation_address,
  null                                                              as relation_province,
  null                                                              as relation_city,
  null                                                              as relation_county,
  null                                                              as corp_type,
  null                                                              as corp_name,
  null                                                              as corp_teleph_nbr,
  null                                                              as corp_fax,
  null                                                              as corp_position,
  cast(create_time as timestamp)                                    as effective_time,
  cast('3000-12-31 00:00:00' as timestamp)                          as expire_time
from (
  select distinct
    create_time,
    get_json_object(requst_data,'$.borrower.id_no') as id_no,
    get_json_object(requst_data,'$.request_no') as request_no,
    get_json_object(requst_data,'$.product_no') as product_id,
    relational_humans
  from ods.t_real_param
  lateral view explode(json_array_to_array(get_json_object(requst_data,'$.relational_humans'))) humans as relational_humans
  where interface_name = 'LOAN_INFO_PER_APPLY'
  and agency_id = '0004'
  and requst_data is not null
  and to_date(create_time) = '2020-03-03'
) as t_real_param
left join (
  select id_no,ecif_no from ecif_core.ecif_customer_hive
) as ecif_customer
on encrypt_aes(t_real_param.id_no) = ecif_customer.id_no
union all
select
  linkman_info.capital_id,
  linkman_info.channel_id,
  linkman_info.project_id,
  linkman_info.product_id,
  linkman_info.cust_id,
  linkman_info.user_hash_no,
  linkman_info.ecif_id,
  linkman_info.due_bill_no,
  linkman_info.linkman_id,
  linkman_info.relational_type,
  linkman_info.relationship,
  linkman_info.relation_idcard_type,
  linkman_info.relation_idcard_no,
  linkman_info.relation_birthday,
  linkman_info.relation_name,
  linkman_info.relation_sex,
  linkman_info.relation_mobile,
  linkman_info.relation_address,
  linkman_info.relation_province,
  linkman_info.relation_city,
  linkman_info.relation_county,
  linkman_info.corp_type,
  linkman_info.corp_name,
  linkman_info.corp_teleph_nbr,
  linkman_info.corp_fax,
  linkman_info.corp_position,
  linkman_info.effective_time,
  cast(if(to_date(linkman_info.expire_time) > '2020-05-06' and resp_log.linkman_id is not null,resp_log.create_time,linkman_info.expire_time) as timestamp) as expire_time
from ods_new_s.linkman_info
left join (
  select
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    concat(get_json_object(standard_req_msg,'$.borrower.id_no'),'_',relational_humans['mbile_phone']) as linkman_id
  from ods.nms_interface_resp_log
  lateral view explode(json_array_to_array(get_json_object(standard_req_msg,'$.relational_humans'))) humans as relational_humans
  where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
  and deal_date = '2020-05-06'
) as resp_log
on linkman_info.linkman_id = resp_log.linkman_id
union all
select
  null                                                                                          as capital_id,
  null                                                                                          as channel_id,
  null                                                                                          as project_id,
  resp_log.product_no                                                                           as product_id,
  null                                                                                          as cust_id,
  resp_log.id_no                                                                                as user_hash_no,
  ecif_no                                                                                       as ecif_id,
  resp_log.apply_no                                                                             as due_bill_no,
  concat(resp_log.id_no,'_',relational_humans['mbile_phone'])                                   as linkman_id,
  case relational_humans['relational_human_type']
  when 'RHT01' then '借款人联系人'
  when 'RHT02' then '共同借款人'
  when 'RHT03' then '抵押人'
  when 'RHT04' then '抵押人家庭成员信息'
  when 'RHT05' then '保证人-个人信用保证'
  else relational_humans['relational_human_type']
  end                                                                                           as relational_type,
  case relational_humans['relationship']
  when 'C' then '配偶'
  when 'F' then '父亲'
  when 'M' then '母亲'
  when 'B' then '兄弟'
  when 'S' then '姐妹'
  when 'L' then '亲属'
  when 'W' then '同事'
  when 'D' then '父母'
  when 'H' then '子女'
  when 'X' then '兄弟姐妹'
  when 'T' then '同学'
  when 'Y' then '朋友'
  when 'O' then '其他'
  else relational_humans['relationship']
  end                                                                                           as relationship,
  case relational_humans['id_type']
  when 'I' then '身份证'
  when 'T' then '台胞证'
  when 'S' then '军官证/士兵证'
  when 'P' then '护照'
  when 'L' then '营业执照'
  when 'O' then '其他有效证件'
  when 'R' then '户口簿'
  when 'H' then '港澳居民来往内地通行证'
  when 'W' then '台湾同胞来往内地通行证'
  when 'F' then '外国人居留证'
  when 'C' then '警官证'
  when 'B' then '外国护照'
  else if(length(relational_humans['id_type']) = 0 or relational_humans['id_type'] is null,null,relational_humans['id_type'])
  end                                                                                           as relation_idcard_type,
  if(length(relational_humans['id_no']) = 0 or relational_humans['id_no'] is null,null,relational_humans['id_no']) as relation_idcard_no,
  if(length(relational_humans['id_no']) = 0 or relational_humans['id_no'] is null,null,datefmt(substring(relational_humans['id_no'],7,8),'yyyyMMdd','yyyy-MM-dd'))  as relation_birthday,
  relational_humans['name']                                                                     as relation_name,
  if(length(relational_humans['id_no']) = 0 or relational_humans['id_no'] is null,null,sex_idno(relational_humans['id_no']))  as relation_sex,
  relational_humans['mbile_phone']                                                              as relation_mobile,
  if(length(relational_humans['address']) = 0 or relational_humans['address'] is null,null,relational_humans['address']) as relation_address,
  if(length(relational_humans['province']) = 0 or relational_humans['province'] is null,null,relational_humans['province']) as relation_province,
  if(length(relational_humans['city']) = 0 or relational_humans['city'] is null,null,relational_humans['city']) as relation_city,
  if(length(relational_humans['area']) = 0 or relational_humans['area'] is null,null,relational_humans['area']) as relation_county,
  null                                                                                          as corp_type,
  null                                                                                          as corp_name,
  null                                                                                          as corp_teleph_nbr,
  null                                                                                          as corp_fax,
  null                                                                                          as corp_position,
  cast(create_time as timestamp)                                                                as effective_time,
  cast('3000-12-31 00:00:00' as timestamp)                                                      as expire_time
from (
  select
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    get_json_object(standard_req_msg,'$.borrower.id_no') as id_no,
    get_json_object(standard_req_msg,'$.apply_no') as apply_no,
    get_json_object(standard_req_msg,'$.product.product_no') as product_no,
    relational_humans
  from ods.nms_interface_resp_log
  lateral view explode(json_array_to_array(get_json_object(standard_req_msg,'$.relational_humans'))) humans as relational_humans
  where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
  and deal_date = '2020-05-06'
) as resp_log
left join (
  select id_no,ecif_no from ecif_core.ecif_customer_hive
) as ecif_customer
on encrypt_aes(resp_log.id_no) = ecif_customer.id_no
limit 10;











select
  null as capital_id,                    -- '资金方编号'
  null as channel_id,                    -- '渠道方编号'
  null as project_id,                    -- '项目编号'
  null as product_id,                    -- '产品编号'
  null as cust_id,                       -- '客户编号（渠道方编号—用户编号）'
  null as user_hash_no,                  -- '用户编号'
  ecif_no as ecif_id,                       -- 'ecif_id'
  loan_id as loan_id,                       -- '借据ID'
  due_bill_no as due_bill_no,                   -- '借据编号'
  contract_no as contract_no,                   -- '合同编号'
  apply_no as apply_no,                      -- '进件编号'
  purpose as loan_usage,                    -- '贷款用途'
  register_date as register_date,                 -- '放款日期'
  request_time as request_time,                  -- '请求时间'
  active_date as loan_active_date,              -- '激活日期—借据生成时间'
  cycle_day as cycle_day,                     -- '账单日'
  loan_expire_date as loan_expire_date,              -- '贷款到期日期'
  loan_init_term as loan_init_term,                -- '贷款期数（3、6、9等）'
  loan_init_prin as loan_init_principal,           -- '贷款本金'
  interest_rate as interest_rate,                 -- '利息利率'
  totle_int as loan_init_interest,            -- '贷款利息'
  term_fee_rate as fee_rate_term,                      -- '手续费费率'
  totle_term_fee as loan_init_term_fee,            -- '贷款手续费'
  svc_fee_rate as fee_rate_svc,                      -- '服务费费率'
  totle_svc_fee as loan_init_svc_fee,             -- '贷款服务费'
  penalty_rate as penalty_rate,                  -- '罚息利率'
  totle_mult_fee as totle_mult_fee,                -- '总应收滞纳金'
  curr_term as loan_term,                     -- '当前期数'
  repay_term as loan_term_repaid,              -- '已还期数'
  remain_term as loan_term_remain,              -- '剩余期数'
  loan_type as loan_type,                     -- '分期类型（R：消费转分期，C：现金分期，B：账单分期，P：POS分期，M：大额分期（专项分期），MCAT：随借随还，MCEP：等额本金，MCEI：等额本息）'
  loan_status as loan_status,                   -- '分期状态（N：正常，O：逾期，F：已还清）'
  if(loan_settle_reason terminal_reason_cd) as paid_out_type,                 -- '结清类型（D：代偿结清，H：回购结清，T：退货（车）结清，P：提前结清，C：强制结清，F：正常到期结清）'
  paid_out_date as paid_out_date,                 -- '还款日期'
  overdue_date as overdue_date,                  -- '逾期起始日期'
  overdue_days as overdue_days,                  -- '逾期天数'
  ((loan_init_prin + totle_int + totle_term_fee + totle_svc_fee) - (paid_principal + paid_interest + paid_svc_fee + paid_term_fee)) as remain_amount,                 -- '剩余金额：本息费'
  (loan_init_prin - paid_principal) as remain_principal,              -- '剩余本金'
  (totle_int - paid_interest) as remain_interest,               -- '剩余利息'
  (due_term_prin + due_term_int + due_penalty + due_term_fee + due_svc_fee + due_mult_amt) as should_repay_amount,           -- '应还金额'
  due_term_prin as should_repay_principal,        -- '应还本金'
  due_term_int as should_repay_interest,         -- '应还利息'
  due_penalty as should_repay_penalty,          -- '应还罚息'
  due_term_fee as should_repay_term_fee,         -- '应还手续费'
  due_svc_fee as should_repay_svc_fee,          -- '应还服务费'
  due_mult_amt as should_repay_mult_amt,         -- '应还滞纳金'
  (paid_principal + paid_interest + paid_penalty + paid_svc_fee + paid_term_fee + paid_mult) as paid_amount,                   -- '已还金额'
  paid_principal as paid_principal,                -- '已还本金'
  paid_interest as paid_interest,                 -- '已还利息'
  paid_penalty as paid_penalty,                  -- '已还罚息'
  paid_svc_fee as paid_svc_fee,                  -- '已还服务费'
  paid_term_fee as paid_term_fee,                 -- '已还手续费'
  paid_mult as paid_mult,                     -- '已还滞纳金'
  (reduce_prin + reduce_interest + reduce_penalty + reduce_svc_fee + reduce_term_fee + reduce_mult_amt) as reduce_amount,                 -- '减免金额'
  reduce_prin as reduce_principal,              -- '减免本金'
  reduce_interest as reduce_interest,               -- '减免利息'
  reduce_penalty as reduce_penalty,                -- '减免罚息'
  reduce_svc_fee as reduce_svc_fee,                -- '减免服务费'
  reduce_term_fee as reduce_term_fee,               -- '减免手续费'
  reduce_mult_amt as reduce_mult_amt,               -- '减免滞纳金'
  overdue_prin as overdue_principal,             -- '逾期本金'
  overdue_interest as overdue_interest,              -- '逾期利息'
  overdue_penalty as overdue_penalty,               -- '逾期罚息'
  overdue_svc_fee as overdue_svc_fee,               -- '逾期服务费'
  overdue_term_fee as overdue_term_fee,              -- '逾期手续费'
  overdue_mult_amt as overdue_mult_amt,              -- '逾期滞纳金'
  first_value() over(partition by order by)  as first_overdue_date,            -- '首个逾期日期'
  null as dpd_begin_date,                -- 'DPD起始日期'
  null as dpd_days,                      -- 'DPD天数'
  null as dpd_days_count,                -- '累计DPD天数'
  max_dpd as dpd_days_max,                  -- '历史最大DPD天数'
  collect_out_date as collect_out_date,              -- '出催日期'
  overdue_term as overdue_term,                  -- '当前逾期期数'
  count_overdue_term as overdue_terms_count,           -- '累计逾期期数'
  max_overdue_term as overdue_terms_max,             -- '历史单次最长逾期期数'
  null as overdue_principal_accumulate,  -- '累计逾期本金'
  max_overdue_prin as overdue_principal_max,         -- '历史最大逾期本金'
  null as mob,                           -- '月账龄'
  sync_date as sync_date,                     -- '同步日期'
  null as loan_map                      -- '借据表中其他字段合集'
  -- as effective_time,                -- '生效时间（yyyy—MM—dd HH:mm:ss）'
  -- as expire_time                    -- '失效时间（yyyy—MM—dd HH:mm:ss）'
from (
  select *
  from ods.ecas_loan
  where d_date = '2020-05-06'
) as ecas_loan
left join (
  select due_bill_no,due_term_prin,due_term_int,due_penalty,due_term_fee,due_svc_fee,due_mult_amt
  from ods.ecas_repay_schedule
  where d_date = '2020-05-06'
) as repay_schedule
on ecas_loan.due_bill_no = repay_schedule.due_bill_no and ecas_loan.curr_term = repay_schedule.curr_term
left join (
  select attr_value,ecif_no
  from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute_hive where attr_key = 'application_no' ) as a
  join ( select inner_id,ecif_no from ecif_core.ecif_inner_id_hive ) as b on a.inner_id = b.inner_id
) as ecif_customer
on ecas_loan.apply_no = ecif_customer.attr_value
limit 10;






select distinct cast(term_fee_rate as double),cast(svc_fee_rate as decimal(15,7)),penalty_rate
from ods.ecas_loan
where due_bill_no = 'DD00023036202001091253006d6449'
limit 10;

select distinct due_bill_no,term_fee_rate,svc_fee_rate,penalty_rate
from ods.ecas_loan
-- where due_bill_no = '1000000181'
limit 10;





select
  -- distinct purpose
  first_value() over(partition by due_bill_no order by cpd_begin_date desc)
from ods.ecas_loan
where cpd_begin_date is not null
limit 10;

select distinct overdue_date
from ods.ecas_loan
limit 10;





select

from temp_today_dwb_loan as tmp
left join (
  select
  a.due_bill_no as due_bill_no,
  min(b.curr_term) as curr_term
  from
  (select * from temp_today_dwb_loan ) a
  left join
  (select * from ods.ecas_repay_schedule where d_date='"+partitionDate+"') b
  on a.due_bill_no=b.due_bill_no
  where (b.pmt_due_date>='"+partitionDate+"' or a.curr_term=b.curr_term) and b.curr_term>0
  group by a.due_bill_no
) as curr_term
on tmp.due_bill_no = curr_term.due_bill_no





select
  ecas_loan.due_bill_no,
  repay_schedule.due_bill_no,
  ecas_loan.curr_term,
  repay_schedule.curr_term
from (select due_bill_no,curr_term from ods.ecas_loan where d_date = '2020-05-06') as ecas_loan
left join (select due_bill_no,curr_term from ods.ecas_repay_schedule where d_date = '2020-05-06') as repay_schedule
on ecas_loan.due_bill_no = repay_schedule.due_bill_no
-- and ecas_loan.curr_term = repay_schedule.curr_term
where ecas_loan.due_bill_no = 'DD0002303620200322103100f4ccaa'
order by ecas_loan.curr_term,repay_schedule.curr_term;


select
a.due_bill_no as due_bill_no,
min(b.curr_term) as curr_term
from
(select due_bill_no,curr_term from ods.ecas_loan where d_date = '2020-05-06') a
left join
(select due_bill_no,curr_term,pmt_due_date from ods.ecas_repay_schedule where d_date = '2020-05-06') b
on a.due_bill_no=b.due_bill_no
where (b.pmt_due_date >= '2020-05-06' or a.curr_term=b.curr_term) and b.curr_term>0
group by a.due_bill_no
limit 10;


select *
from ods.ecas_loan
where due_bill_no = '1000000181'
limit 10;




desc ods.ecas_repay_schedule;


select distinct d_date
from ods.ecas_repay_schedule
order by d_date;




select distinct
  due_bill_no,
  curr_term,
  grace_date,
  start_interest_date,
  pmt_due_date,
  origin_pmt_due_date,
  paid_out_type,
  case schedule_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else schedule_status
  end as schedule_status,
  paid_out_date,
  -- datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as lst_upd_time
from ods.ecas_repay_schedule
where due_bill_no = '1000000002'
and lst_upd_time is not null
order by pmt_due_date,lst_upd_time desc
-- limit 10
;

select distinct
  due_bill_no,
  curr_term,
  pmt_due_date,
  origin_pmt_due_date,
  grace_date,
  due_term_prin,
  due_term_int,
  due_penalty,
  due_term_fee,
  due_svc_fee,
  due_mult_amt
from ods.ecas_repay_schedule
where lst_upd_time is not null
and due_bill_no = '1000000002'
order by pmt_due_date
-- ,lst_upd_time desc
-- limit 10
;



select distinct
  due_bill_no,
  case bnp_type
  when 'Pricinpal'         then '本金'
  when 'Interest'          then '利息'
  when 'Penalty'           then '罚息'
  when 'SVCFee'            then '服务费'
  when 'TERMFee'           then '手续费'
  when 'LatePaymentCharge' then '滞纳金'

  when 'Mulct'             then '罚金'
  when 'Compound'          then '复利'
  when 'CardFee'           then '年费'
  when 'OverLimitFee'      then '超限费'
  when 'NSFCharge'         then '资金不足罚金'
  when 'TXNFee'            then '交易费'
  when 'LifeInsuFee'       then '寿险计划包费'
  else bnp_type
  end as bnp_type,
  repay_amt,
  term,
  txn_date,
  overdue_days,
  case loan_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else loan_status
  end as loan_status,
  datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as lst_upd_time
from ods.ecas_repay_hst
where due_bill_no = '1000000002'
order by term,bnp_type desc
;



select
  due_bill_no,
  sum(if(bnp_type = 'Pricinpal',        repay_amt,0)) as repaid_pincipal,
  sum(if(bnp_type = 'Interest',         repay_amt,0)) as repaid_interest,
  sum(if(bnp_type = 'Penalty',          repay_amt,0)) as repaid_penalty_interest,
  sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as repaid_svc_fee,
  sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as repaid_term_fee,
  sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as repaid_mult,
  term,
  txn_date
from (
  select distinct
    due_bill_no,
    bnp_type,
    repay_amt,
    term,
    cast(txn_date as timestamp) as txn_date
  from ods.ecas_repay_hst
  where due_bill_no = '1000000002'
) as tmp
group by due_bill_no,term,txn_date
order by term,txn_date desc
;




set hivevar:compute_date='2020-03-03';



invalidate metadata ods_new_s.loan_info;

refresh ods_new_s.loan_info;
select * from ods_new_s.loan_info;

invalidate metadata ods_new_s.loan_info_tmp;

refresh ods_new_s.loan_info_tmp;
select * from ods_new_s.loan_info_tmp;


ALTER TABLE ods_new_s.loan_info DROP IF EXISTS partition (is_settled = 'is');

select distinct
  due_bill_no,
  curr_term,
  datefmt(lst_upd_time,'ms','yyyy-MM-dd') as update_time,
  sync_date,
  d_date
from ods.ecas_loan
where sync_date is not null
and (
  d_date = '2020-02-25' or
  d_date = '2020-02-26' or
  d_date = '2020-02-27' or
  d_date = '2020-02-28' or
  d_date = '2020-02-29' or
  d_date = '2020-03-01' or
  d_date = '2020-03-02' or
  d_date = '2020-03-03'
)
and due_bill_no = '1000000002'
order by update_time
-- limit 10
;



select distinct
  loan_status
from ods.ccs_loan
limit 10
;

invalidate metadata;
select distinct
  due_bill_no,
  -- schedule_id,
  curr_term,
  -- create_time,lst_upd_time,
  -- first_value(lst_upd_time) over(partition by due_bill_no,schedule_id,curr_term order by lst_upd_time) as update_time
 datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
 datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
 d_date
 -- length(lst_upd_time) as len
from ods.ecas_repay_schedule
where d_date != 'bak'
-- and lst_upd_time is null
-- and due_bill_no in ('DD0002303620200109111600e8631a')
-- and schedule_id = '000015785673691admin000077000014'
and d_date = '2019-12-02'
order by due_bill_no,curr_term,create_time,update_time,d_date
limit 100
;



invalidate metadata;
select * from ods_new_s.repay_schedule limit 10;



select distinct *
from ods.ecas_repay_schedule
where d_date != 'bak'
-- and schedule_id = '000015785673691admin000077000014'
and d_date = '2020-05-09'
and (lst_upd_time is null or create_time is null)
order by d_date
-- limit 100
;

select distinct
  -- due_bill_no,
  md5(
    concat(
      cast(cast(if(schedule_id                  is null,'',schedule_id)          as string)         as string),
      cast(cast(if(out_side_schedule_no         is null,'',out_side_schedule_no) as string)         as string),
      cast(cast(if(due_bill_no                  is null,'',due_bill_no)          as string)         as string),
      cast(cast(if(loan_init_prin               is null,0, loan_init_prin)       as decimal(15,4))  as string),
      cast(cast(if(loan_init_term               is null,0, loan_init_term)       as decimal(3,0))   as string),
      cast(cast(if(curr_term                    is null,0, curr_term)            as decimal(3,0))   as string),
      cast(to_date(cast(if(start_interest_date  is null,'',start_interest_date)  as timestamp))     as string),
      cast(to_date(cast(if(pmt_due_date         is null,'',pmt_due_date)         as timestamp))     as string),
      cast(to_date(cast(if(origin_pmt_due_date  is null,'',origin_pmt_due_date)  as timestamp))     as string),
      cast(to_date(cast(if(grace_date           is null,'',grace_date)           as timestamp))     as string),
      cast(cast(if(due_term_prin                is null,0 ,due_term_prin)        as decimal(15,4))  as string),
      cast(cast(if(due_term_int                 is null,0 ,due_term_int)         as decimal(15,4))  as string),
      cast(cast(if(due_penalty                  is null,0 ,due_penalty)          as decimal(15,4))  as string),
      cast(cast(if(due_term_fee                 is null,0 ,due_term_fee)         as decimal(15,4))  as string),
      cast(cast(if(due_svc_fee                  is null,0 ,due_svc_fee)          as decimal(15,4))  as string),
      cast(cast(if(due_mult_amt                 is null,0 ,due_mult_amt)         as decimal(15,4))  as string),
      cast(cast(if(reduced_amt                  is null,0 ,reduced_amt)          as decimal(15,4))  as string),
      cast(cast(if(reduce_term_prin             is null,0 ,reduce_term_prin)     as decimal(15,4))  as string),
      cast(cast(if(reduce_term_int              is null,0 ,reduce_term_int)      as decimal(15,4))  as string),
      cast(cast(if(reduce_term_fee              is null,0 ,reduce_term_fee)      as decimal(15,4))  as string),
      cast(cast(if(reduce_svc_fee               is null,0 ,reduce_svc_fee)       as decimal(15,4))  as string),
      cast(cast(if(reduce_penalty               is null,0 ,reduce_penalty)       as decimal(15,4))  as string),
      cast(cast(if(reduce_mult_amt              is null,0 ,reduce_mult_amt)      as decimal(15,4))  as string)
    )
  ) as md_5,
  concat(
    cast(cast(if(schedule_id                  is null,'',schedule_id)          as string)         as string),
    cast(cast(if(out_side_schedule_no         is null,'',out_side_schedule_no) as string)         as string),
    cast(cast(if(due_bill_no                  is null,'',due_bill_no)          as string)         as string),
    cast(cast(if(loan_init_prin               is null,0, loan_init_prin)       as decimal(15,4))  as string),
    cast(cast(if(loan_init_term               is null,0, loan_init_term)       as decimal(3,0))   as string),
    cast(cast(if(curr_term                    is null,0, curr_term)            as decimal(3,0))   as string),
    cast(to_date(cast(if(start_interest_date  is null,'',start_interest_date)  as timestamp))     as string),
    cast(to_date(cast(if(pmt_due_date         is null,'',pmt_due_date)         as timestamp))     as string),
    cast(to_date(cast(if(origin_pmt_due_date  is null,'',origin_pmt_due_date)  as timestamp))     as string),
    cast(to_date(cast(if(grace_date           is null,'',grace_date)           as timestamp))     as string),
    cast(cast(if(due_term_prin                is null,0 ,due_term_prin)        as decimal(15,4))  as string),
    cast(cast(if(due_term_int                 is null,0 ,due_term_int)         as decimal(15,4))  as string),
    cast(cast(if(due_penalty                  is null,0 ,due_penalty)          as decimal(15,4))  as string),
    cast(cast(if(due_term_fee                 is null,0 ,due_term_fee)         as decimal(15,4))  as string),
    cast(cast(if(due_svc_fee                  is null,0 ,due_svc_fee)          as decimal(15,4))  as string),
    cast(cast(if(due_mult_amt                 is null,0 ,due_mult_amt)         as decimal(15,4))  as string),
    cast(cast(if(reduced_amt                  is null,0 ,reduced_amt)          as decimal(15,4))  as string),
    cast(cast(if(reduce_term_prin             is null,0 ,reduce_term_prin)     as decimal(15,4))  as string),
    cast(cast(if(reduce_term_int              is null,0 ,reduce_term_int)      as decimal(15,4))  as string),
    cast(cast(if(reduce_term_fee              is null,0 ,reduce_term_fee)      as decimal(15,4))  as string),
    cast(cast(if(reduce_svc_fee               is null,0 ,reduce_svc_fee)       as decimal(15,4))  as string),
    cast(cast(if(reduce_penalty               is null,0 ,reduce_penalty)       as decimal(15,4))  as string),
    cast(cast(if(reduce_mult_amt              is null,0 ,reduce_mult_amt)      as decimal(15,4))  as string)
  ) as con
from ods.ecas_repay_schedule
where due_bill_no = 'DD00023036201910110921008781fb'
order by md_5
-- limit 10
;



select
  -- schedule_id,due_bill_no,
  md5(concat_col) as repay_schedule_md5
  ,create_time,update_time
  ,concat_col
from (
  select distinct
    -- schedule_id,due_bill_no,
    concat_ws('_',
      cast(cast(if(due_bill_no          is null,'',due_bill_no)           as string)         as string),
      cast(cast(if(schedule_id          is null,'',schedule_id)           as string)         as string),
      cast(cast(if(out_side_schedule_no is null,'',out_side_schedule_no)  as string)         as string),
      cast(cast(if(loan_init_prin       is null,0 ,loan_init_prin)        as decimal(15,4))  as string),
      cast(cast(if(loan_init_term       is null,0 ,loan_init_term)        as decimal(3,0))   as string),
      cast(cast(if(curr_term            is null,0 ,curr_term)             as decimal(3,0))   as string),
      cast(cast(if(start_interest_date  is null,'',start_interest_date)   as string)         as string),
      cast(cast(if(pmt_due_date         is null,'',pmt_due_date)          as string)         as string),
      cast(cast(if(origin_pmt_due_date  is null,'',origin_pmt_due_date)   as string)         as string),
      cast(cast(if(grace_date           is null,'',grace_date)            as string)         as string),
      cast(cast(if(due_term_prin        is null,0 ,due_term_prin)         as decimal(15,4))  as string),
      cast(cast(if(due_term_int         is null,0 ,due_term_int)          as decimal(15,4))  as string),
      cast(cast(if(due_penalty          is null,0 ,due_penalty)           as decimal(15,4))  as string),
      cast(cast(if(due_term_fee         is null,0 ,due_term_fee)          as decimal(15,4))  as string),
      cast(cast(if(due_svc_fee          is null,0 ,due_svc_fee)           as decimal(15,4))  as string),
      cast(cast(if(due_mult_amt         is null,0 ,due_mult_amt)          as decimal(15,4))  as string),
      cast(cast(if(reduced_amt          is null,0 ,reduced_amt)           as decimal(15,4))  as string),
      cast(cast(if(reduce_term_prin     is null,0 ,reduce_term_prin)      as decimal(15,4))  as string),
      cast(cast(if(reduce_term_int      is null,0 ,reduce_term_int)       as decimal(15,4))  as string),
      cast(cast(if(reduce_term_fee      is null,0 ,reduce_term_fee)       as decimal(15,4))  as string),
      cast(cast(if(reduce_svc_fee       is null,0 ,reduce_svc_fee)        as decimal(15,4))  as string),
      cast(cast(if(reduce_penalty       is null,0 ,reduce_penalty)        as decimal(15,4))  as string),
      cast(cast(if(reduce_mult_amt      is null,0 ,reduce_mult_amt)       as decimal(15,4))  as string)
    ) as concat_col,
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time
  from ods.ecas_repay_schedule
  where create_time is not null
) as repay_schedule
order by concat_col,repay_schedule_md5
limit 50
;




select distinct
  cast(cast(if(start_interest_date  is null,'',start_interest_date)   as string)         as string) as start_interest_date,
  cast(cast(if(pmt_due_date         is null,'',pmt_due_date)          as string)         as string) as pmt_due_date,
  cast(cast(if(origin_pmt_due_date  is null,'',origin_pmt_due_date)   as string)         as string) as origin_pmt_due_date,
  cast(cast(if(grace_date           is null,'',grace_date)            as string)         as string) as grace_date
from ods.ecas_repay_schedule
where create_time is not null
and due_bill_no = '1000000002'
;




select count(distinct schedule_id) as cnt
from ods.ecas_repay_schedule
where create_time is not null
and d_date = '2020-05-09'
;

set hivevar:compute_date='2020-04-20';
select count(distinct schedule_id) as cnt
from ods.ecas_repay_schedule
where create_time is not null
and (datefmt(create_time,'ms','yyyy-MM-dd') = ${compute_date} or datefmt(lst_upd_time,'ms','yyyy-MM-dd') = ${compute_date})
;


select count(distinct schedule_id) as cnt
from ods_new_s.repay_schedule
;

invalidate metadata ods_new_s.repay_schedule;
select
  *
from ods_new_s.repay_schedule
order by due_bill_no,loan_term,effective_time;


select
  min(effective_time) as effective_time
from ods_new_s.repay_schedule


select
  -- min(cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)) as create_time,
  min(d_date) as d_date
from ods.ecas_repay_schedule
where create_time is not null
;


select distinct
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time
from ods.ecas_repay_schedule
where create_time is not null
and d_date = '2020-04-30'
limit 10;





select distinct
  d_date
from ods.ccs_repay_schedule
order by d_date
limit 10;


select
  -- distinct
  *,
  -- d_date,
  datefmt(create_time,'ms','yyyy-MM-dd')  as create_time,
  datefmt(lst_upd_time,'ms','yyyy-MM-dd') as update_time
  -- count(distinct schedule_id) as schedule_cnt,
  -- count(1) as cnt
from ods.ccs_repay_schedule
-- where d_date in ('2020-03-01','2020-03-02')
-- where d_date in ('2020-05-10','2020-05-11')
-- and schedule_id = '000015756247871admin000006000004'
-- where schedule_id = '000015756247871admin000006000004'
-- group by d_date,datefmt(create_time,'ms','yyyy-MM-dd'),datefmt(lst_upd_time,'ms','yyyy-MM-dd')
order by d_date,create_time,update_time
limit 10
;


select
  d_date,
  count(distinct schedule_id) as schedule_cnt,
  count(1) as cnt
from ods.ccs_repay_schedule
-- where d_date in ('2020-03-01','2020-03-02')
-- and (
--   datefmt(create_time,'ms','yyyy-MM-dd')  = '2020-03-02' or
--   datefmt(lst_upd_time,'ms','yyyy-MM-dd') = '2020-03-02')
group by d_date
order by d_date
-- limit 10
;


select distinct
  d_date
  -- count(distinct schedule_id) as schedule_cnt,
  -- count(1) as cnt,
  -- if(count(distinct schedule_id) != count(1),'---','|||') as flag,
  -- datefmt(if(create_time is null,cast(create_user as bigint),create_time),'ms','yyyy-MM-dd')  as create_time,
  -- datefmt(lst_upd_time,'ms','yyyy-MM-dd') as update_time
from ods.ecas_repay_schedule
where d_date != 'bak'
and create_time is null
-- and d_date = '2020-03-01'
-- group by d_date
order by d_date
limit 10
;

invalidate metadata ods_new_s.repay_schedule;
select count(1) from ods_new_s.repay_schedule;

select distinct
  product_code
from dwb.dwb_loan
limit 10
;

select distinct
  loan_code
from ods.ccs_loan
limit 10
;

select (
select if(create_time is null,curr_time,create_time) as create_time from (select distinct create_time from dim_new.biz_conf where product_id = '001507') as t1 full join (select current_timestamp as curr_time) as t2
) as tt
;



select * from ods.ccs_loan limit 10;


select distinct loan_id,ref_nbr from ods.ccs_repay_schedule limit 50;


invalidate metadata;
select
  count(1) as cnt
from ods_new_s.linkman_info_tmp;

select distinct * from
(
  select a,first_value(b) over(partition by a order by b desc) as t
  from (
    select 1 as a,null as b union all
    select null as a,2 as b union all
    select 1 as a,null as b union all
    select null as a,null as b
  ) as t
) as t
;


select distinct
  sync_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time
from ods.ecas_loan
order by sync_date
limit 10
;



select distinct *
from ods.ecas_loan
where datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') = '2019-12-30 20:00:18'
limit 10
;


select distinct
  curr_term,
  ref_nbr,
  loan_pmt_due_date,
  stmt_date
from ods.ccs_repay_schedule
limit 10
;



select distinct
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time
  -- force_flag
  -- due_bill_no,
  -- loan_init_term,
  -- curr_term,
  -- remain_term
from ods.ccs_loan
-- where due_bill_no = '64e9950900c447ee89c1d705da108fe6'
limit 10
;





select distinct *
from ods.ccs_loan
where loan_id = '000015654698161admin000186000000'
-- where due_bill_no = '01547cdafffe4ab5a78b39d77c2c57eb'
limit 10
;



select attr_value,ecif_no as ecif_id
from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute where attr_key = 'application_no' ) as a
join ( select inner_id,ecif_no from ecif_core.ecif_inner ) as b on a.inner_id = b.inner_id
where attr_value = '01547cdafffe4ab5a78b39d77c2c57eb'
;



select distinct
  ref_nbr,
  day(loan_pmt_due_date) as cycle_day
from ods.ccs_repay_schedule
-- where ref_nbr = '000015654698111admin000106000008'
where loan_id = '000015654698161admin000186000000'
limit 10
;


select nvl(null,0) as a;
select nvl(1,0) as a;





select
  count(1) as cnt
from ods_new_s.loan_info
-- where to_date(effective_time) <= '2020-03-01'
limit 10
;





select
  count(1) as cnt
from
(
select
  -- *
  distinct
  to_date(effective_time) as effective_time
from ods_new_s.loan_info
where is_settled = 'no'
  -- and cast(to_date(effective_time) as string) = '2020-03-02'
) as t
limit 10
;

select from_unixtime(unix_timestamp('2020-05-14','yyyy-MM-dd') + 113400,'yyyy-MM-dd HH:mm:ss');


select count(1) as cnt from ods_new_s.loan_info_tmp;

select count(1) as cnt from ods_new_s.loan_info;



select
  count(1) as cnt
from (
  select distinct *
  from ods_new_s.loan_info
  -- where due_bill_no = 'd45f403834914fec94c5aafe4ac26c6a'
) as t
;


select
  count(1) as cnt
from (
  select distinct *
  from ods_new_s.loan_info_tmp
  -- where due_bill_no = 'd45f403834914fec94c5aafe4ac26c6a'
) as t
;


select distinct loan_info.due_bill_no
from ods_new_s.loan_info
left join ods_new_s.loan_info_tmp
on  loan_info.due_bill_no = loan_info_tmp.due_bill_no
and loan_info.loan_term   = loan_info_tmp.loan_term
and to_date(loan_info.effective_time) = to_date(loan_info_tmp.effective_time)
where loan_info_tmp.due_bill_no is null
-- limit 10
;


select distinct loan_info_tmp.due_bill_no
from ods_new_s.loan_info_tmp
left join ods_new_s.loan_info
on  loan_info.due_bill_no = loan_info_tmp.due_bill_no
and loan_info.loan_term   = loan_info_tmp.loan_term
and loan_info.expire_time = loan_info_tmp.expire_time
where loan_info.due_bill_no is null
;



select *
from ods_new_s.loan_info
-- where due_bill_no is null
where due_bill_no = 'DD0002303620191028183200434c28'
;

select *
from ods_new_s.loan_info_tmp as loan_info
-- where due_bill_no is null
where due_bill_no = '1000000014'
;


select distinct
  d_date,
  loan_code,
  loan_id,
  due_bill_no,
  purpose,
  register_date,
  cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as request_time,
  active_date,
  loan_expire_date,
  loan_type,
  loan_init_term,
  curr_term,
  remain_term,
  loan_status,
  terminal_reason_cd,
  paid_out_date,
  terminal_date,
  loan_init_prin,
  interest_rate,
  pay_interest,
  fee_rate,
  loan_init_fee,
  installment_fee_rate,
  tol_svc_fee,
  penalty_rate,
  paid_fee,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  (loan_init_prin + pay_interest + loan_init_fee + tol_svc_fee - paid_fee) as remain_amount,
  (loan_init_prin - paid_principal)                                        as remain_principal,
  (pay_interest - paid_interest)                                           as remain_interest,
  overdue_date,
  -- if(overdue_date is null,0,datediff(current_date,overdue_date))           as overdue_days,
  max_dpd,
  collect_out_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)       as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as update_time
from ods.ccs_loan
-- where due_bill_no = 'd45f403834914fec94c5aafe4ac26c6a'
where due_bill_no is null
;


select distinct
  product_code                                                        as product_id,
  loan_id                                                             as loan_id,
  due_bill_no                                                         as due_bill_no,
  contract_no                                                         as contract_no,
  apply_no                                                            as apply_no,
  purpose                                                             as loan_usage,
  register_date                                                       as register_date,
  cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as request_time,
  active_date                                                         as loan_active_date,
  cast(cycle_day as decimal(2,0))                                     as cycle_day,
  loan_expire_date                                                    as loan_expire_date,
  loan_type                                                           as loan_type,
  loan_init_term                                                      as loan_init_term,
  curr_term                                                           as loan_term,
  repay_term                                                          as loan_term_repaid,
  remain_term                                                         as loan_term_remain,
  loan_status                                                         as loan_status,
  terminal_reason_cd                                                  as loan_out_reason,
  loan_settle_reason                                                  as paid_out_type,
  paid_out_date                                                       as paid_out_date,
  terminal_date                                                       as terminal_date,
  loan_init_prin                                                      as loan_init_principal,
  interest_rate                                                       as loan_init_interest_rate,
  totle_int                                                           as loan_init_interest,
  term_fee_rate                                                       as loan_init_term_fee_rate,
  totle_term_fee                                                      as loan_init_term_fee,
  svc_fee_rate                                                        as loan_init_svc_fee_rate,
  totle_svc_fee                                                       as loan_init_svc_fee,
  penalty_rate                                                        as loan_init_penalty_rate,
  paid_principal                                                      as paid_principal,
  paid_interest                                                       as paid_interest,
  paid_penalty                                                        as paid_penalty,
  paid_svc_fee                                                        as paid_svc_fee,
  paid_term_fee                                                       as paid_term_fee,
  paid_mult                                                           as paid_mult,
  overdue_prin                                                        as overdue_principal,
  overdue_interest                                                    as overdue_interest,
  overdue_svc_fee                                                     as overdue_svc_fee,
  overdue_term_fee                                                    as overdue_term_fee,
  overdue_penalty                                                     as overdue_penalty,
  overdue_mult_amt                                                    as overdue_mult_amt,
  overdue_date                                                        as overdue_date,
  overdue_days                                                        as overdue_days,
  max_dpd                                                             as dpd_days_max,
  collect_out_date                                                    as collect_out_date,
  overdue_term                                                        as overdue_term,
  count_overdue_term                                                  as overdue_terms_count,
  max_overdue_term                                                    as overdue_terms_max,
  max_overdue_prin                                                    as overdue_principal_max,
  sync_date                                                           as sync_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
  d_date
from ods.ecas_loan
where d_date != 'bak'
  and due_bill_no = 'DD0002303620191104134100fda519'
  and d_date = '2020-03-02'
-- where due_bill_no is null
;


select
  count(1) as cnt
from
(
select distinct
  d_date,
  loan_code,
  loan_id,
  due_bill_no,
  purpose,
  register_date,
  cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as request_time,
  active_date,
  loan_expire_date,
  loan_type,
  loan_init_term,
  curr_term,
  remain_term,
  loan_status,
  terminal_reason_cd,
  paid_out_date,
  terminal_date,
  loan_init_prin,
  interest_rate,
  pay_interest,
  fee_rate,
  loan_init_fee,
  installment_fee_rate,
  tol_svc_fee,
  penalty_rate,
  paid_fee,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  (loan_init_prin + pay_interest + loan_init_fee + tol_svc_fee - paid_fee) as remain_amount,
  (loan_init_prin - paid_principal)                                        as remain_principal,
  (pay_interest - paid_interest)                                           as remain_interest,
  overdue_date,
  -- if(overdue_date is null,0,datediff(current_date,overdue_date))           as overdue_days,
  max_dpd,
  collect_out_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)       as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as update_time
from ods.ccs_loan
-- where due_bill_no = 'd45f403834914fec94c5aafe4ac26c6a'
where d_date = '2020-03-02'
) as t
;


select
  count(1) as cnt
from
(
select distinct
  product_code                                                        as product_id,
  loan_id                                                             as loan_id,
  due_bill_no                                                         as due_bill_no,
  contract_no                                                         as contract_no,
  apply_no                                                            as apply_no,
  purpose                                                             as loan_usage,
  register_date                                                       as register_date,
  cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as request_time,
  active_date                                                         as loan_active_date,
  cast(cycle_day as decimal(2,0))                                     as cycle_day,
  loan_expire_date                                                    as loan_expire_date,
  loan_type                                                           as loan_type,
  loan_init_term                                                      as loan_init_term,
  curr_term                                                           as loan_term,
  repay_term                                                          as loan_term_repaid,
  remain_term                                                         as loan_term_remain,
  loan_status                                                         as loan_status,
  terminal_reason_cd                                                  as loan_out_reason,
  loan_settle_reason                                                  as paid_out_type,
  paid_out_date                                                       as paid_out_date,
  terminal_date                                                       as terminal_date,
  loan_init_prin                                                      as loan_init_principal,
  interest_rate                                                       as loan_init_interest_rate,
  totle_int                                                           as loan_init_interest,
  term_fee_rate                                                       as loan_init_term_fee_rate,
  totle_term_fee                                                      as loan_init_term_fee,
  svc_fee_rate                                                        as loan_init_svc_fee_rate,
  totle_svc_fee                                                       as loan_init_svc_fee,
  penalty_rate                                                        as loan_init_penalty_rate,
  paid_principal                                                      as paid_principal,
  paid_interest                                                       as paid_interest,
  paid_penalty                                                        as paid_penalty,
  paid_svc_fee                                                        as paid_svc_fee,
  paid_term_fee                                                       as paid_term_fee,
  paid_mult                                                           as paid_mult,
  overdue_prin                                                        as overdue_principal,
  overdue_interest                                                    as overdue_interest,
  overdue_svc_fee                                                     as overdue_svc_fee,
  overdue_term_fee                                                    as overdue_term_fee,
  overdue_penalty                                                     as overdue_penalty,
  overdue_mult_amt                                                    as overdue_mult_amt,
  overdue_date                                                        as overdue_date,
  overdue_days                                                        as overdue_days,
  max_dpd                                                             as dpd_days_max,
  collect_out_date                                                    as collect_out_date,
  overdue_term                                                        as overdue_term,
  count_overdue_term                                                  as overdue_terms_count,
  max_overdue_term                                                    as overdue_terms_max,
  max_overdue_prin                                                    as overdue_principal_max,
  sync_date                                                           as sync_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
  d_date
from ods.ecas_loan
where d_date != 'bak'
  and due_bill_no = 'DD00023036201911281430009499a9'
  and curr_term = 3
  and d_date = '2020-03-02'
) as t
;

select count(1) as cnt from ods_new_s.loan_info_tmp;

select
from ods_new_s.loan_info
left join ods_new_s.loan_info_tmp
on  loan_info.due_bill_no = loan_info_tmp.due_bill_no
and loan_info.loan_term   = loan_info_tmp.loan_term
and loan_info.expire_time = loan_info_tmp.expire_time
;


select due_bill_no,loan_term
from ods_new_s.loan_info_tmp
group by due_bill_no,loan_term
having count(due_bill_no) > 1
;


select due_bill_no,loan_term
from ods_new_s.loan_info
group by due_bill_no,loan_term
having count(due_bill_no) > 1
;



select *
from ods_new_s.loan_info
where due_bill_no = 'DD00023036201911281430009499a9'
;

select *
from ods_new_s.loan_info_tmp as loan_info
where due_bill_no = 'DD00023036201911281430009499a9'
;




select * FROM ods.ecas_loan
WHERE d_date = to_date(date_sub(current_timestamp(),7))
LIMIT 10;


select to_date(date_sub(current_timestamp(),7))
;

select
  -- org,
  -- deal_date,
  -- create_time,
  -- update_time,
  original_msg
from ods.ecas_msg_log
where msg_type = 'CREDIT_APPLY'
limit 10
;


invalidate metadata;
refresh ods_new_s.repay_detail;
select biz_date,count(1) as cnt
from ods_new_s.repay_detail
where biz_date = '2020-07-01'
group by biz_date
order by biz_date
;



select * from ods_new_s.repay_detail
limit 10;





select distinct
  -- id,
  -- deal_date,
  -- create_time,
  -- update_time,
  -- req_log_id,
  -- org,
  -- standard_req_msg,
  -- standard_resp_msg,
  -- status
  -- sta_service_method_name
  -- standard_req_msg
  -- get_json_object(standard_req_msg,'$.product.currency_amt') as currency_amt,
  -- get_json_object(standard_req_msg,'$.product.loan_amt') as loan_amt
  -- standard_resp_msg
  get_json_object(standard_resp_msg,'$.acct_setup_ind') as acct_setup_ind
from ods.nms_interface_resp_log
where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
limit 10
;

select
  standard_req_msg
from ods.nms_interface_resp_log
where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
limit 1
;


select if(a < -1,a,0) as t
from (
  select 1    as a union all
  select -2   as a union all
  select null as a
) as tmp
;


select unix_timestamp(a) as t
from (
  select from_unixtime(1591018771,'yyyy-MM-dd HH:mm:ss') as a union all
  select from_unixtime(1591018748,'yyyy-MM-dd HH:mm:ss') as a union all
  select from_unixtime(null,'yyyy-MM-dd HH:mm:ss') as a
) as tmp
;


select to_unix_timestamp(current_timestamp) as t,from_unixtime(to_unix_timestamp(current_timestamp),'yyyy-MM-dd HH:mm:ss') as t;



show partitions ods.ecas_loan;


truncate table ods_new_s.linkman_info_tmp;


select * from ods_new_s.customer_info;
show partitions ods_new_s.customer_info;

select
  product_id,
  count(1) as cnt
from ods_new_s.customer_info
group by product_id
;


select
  count(1) as cnt
from ods_new_s.customer_info
;

select
  count(1) as cnt
from ods_new_s.user_info
;



select
  product_id,
  due_bill_no,
  loan_active_date,
  loan_init_term,
  loan_term,
  loan_term_repaid,
  loan_term_remain,
  overdue_days,
  overdue_terms_count,
  loan_init_principal,
  remain_principal
  ,overdue_days
  ,effective_time
  ,expire_time
  ,to_date(expire_time) as expire_date
from ods_new_s.loan_info
-- from ods_new_s.loan_info_tmp
where due_bill_no = '1000000106'
  -- and loan_active_date = '2020-06-01'
  -- and to_date(expire_time) = '3000-12-31'
order by effective_time
;




select distinct
  *
from ods_new_s.loan_info
-- from ods_new_s.loan_info_tmp
where due_bill_no = '1000000106'
  -- and loan_active_date = '2020-06-01'
  -- and to_date(expire_time) = '3000-12-31'
order by effective_time
;

select distinct
  *
from ods_new_s.repay_schedule
-- from ods_new_s.loan_info_tmp
where due_bill_no = '1000000106'
  -- and loan_active_date = '2020-06-01'
  -- and to_date(expire_time) = '3000-12-31'
order by loan_term,effective_time
;


select min(d_date)
from ods.ecas_loan
-- where due_bill_no = '1000000106'
-- order by d_date
;




set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=5000;



select min(d_date) as d_date
from ods.ecas_repay_hst
;

select min(d_date) as d_date
from ods.ecas_repay_schedule
;

select count(1) as cnt
from ods.ecas_repay_schedule
where d_date = '2020-06-01'
;

select count(1) as cnt
from ods.ecas_repay_schedule
where d_date = date_sub('2020-06-01',1)
;

select count(1) as cnt
from
(
select distinct
  due_bill_no                         as due_bill_no,
  loan_init_prin                      as loan_init_principal,
  loan_init_term                      as loan_init_term,
  curr_term                           as loan_term,
  start_interest_date                 as start_interest_date,
  pmt_due_date                        as should_repay_date,
  origin_pmt_due_date                 as should_repay_date_history,
  grace_date                          as grace_date,
  due_term_prin                       as should_repay_principal,
  due_term_int                        as should_repay_interest,
  due_penalty                         as should_repay_penalty,
  due_term_fee                        as should_repay_term_fee,
  due_svc_fee                         as should_repay_svc_fee,
  due_mult_amt                        as should_repay_mult_amt,
  reduced_amt                         as reduce_amount,
  reduce_term_prin                    as reduce_principal,
  reduce_term_int                     as reduce_interest,
  reduce_term_fee                     as reduce_term_fee,
  reduce_svc_fee                      as reduce_svc_fee,
  reduce_penalty                      as reduce_penalty,
  reduce_mult_amt                     as reduce_mult_amt,
  is_empty(cast(lst_upd_time as string),cast(lst_upd_user as string)) as update_time
from ods.ecas_repay_schedule
where due_bill_no = 'DD0002303620191101143800dfc047'
order by due_bill_no,loan_term,update_time
-- where d_date = '2020-06-01'
-- where d_date = date_sub('2020-06-01',1)
) as tmp
;


select count(1) as cnt
from
(
select
  product_id,
  due_bill_no,
  schedule_id,
  out_side_schedule_no,
  loan_init_principal,
  loan_init_term,
  loan_term,
  start_interest_date,
  should_repay_date,
  should_repay_date_history,
  grace_date,
  should_repay_principal,
  should_repay_interest,
  should_repay_penalty,
  should_repay_term_fee,
  should_repay_svc_fee,
  should_repay_mult_amt,
  reduce_amount,
  reduce_principal,
  reduce_interest,
  reduce_term_fee,
  reduce_svc_fee,
  reduce_penalty,
  reduce_mult_amt,
  effective_time,
  expire_time
from ods_new_s.repay_schedule
where due_bill_no = 'DD0002303620191101143800dfc047'
-- -- where d_date = '2020-06-01'
-- where d_date = date_sub('2020-06-01',1)
) as tmp
;








select count(1) as cnt
-- from ods_new_s.repay_detail
from ods_new_s.repay_detail_tmp
;

show partitions ods_new_s.repay_detail;

select
count(1) as cnt
from
(
select
  biz_date,
  product_id,
  due_bill_no,
  repay_term,
  loan_status_cn,
  overdue_days,
  bnp_type_cn,
  repay_amount,
  batch_date,
  create_time,
  update_time
from ods_new_s.repay_detail
order by biz_date,product_id,due_bill_no,repay_term
) as tmp
limit 10
;



select
  product_id,
  due_bill_no,
  loan_init_principal,
  loan_init_term,
  loan_term,
  start_interest_date,
  should_repay_date,
  should_repay_date_history,
  grace_date,
  should_repay_principal,
  should_repay_interest,
  should_repay_penalty,
  reduce_amount,
  reduce_principal,
  reduce_interest,
  reduce_penalty,
  effective_time,
  expire_time
from ods_new_s.repay_schedule
-- group by due_bill_no,loan_term
-- having count(distinct due_bill_no,loan_term) > 1
order by product_id,due_bill_no,loan_term
limit 100
;



select count(1) as cnt
from ods.ecas_repay_schedule
;


select count(1) as cnt
from ods_new_s.customer_info
;

select count(1) as cnt
from ods.ecas_msg_log
;

select count(1) as cnt
from ods.ecas_msg_log_bak
;

select min(d_date) as cnt
from ods.ecas_loan
;


select min(deal_date) as cnt
from ods.nms_interface_resp_log
;


select min(datefmt(update_time,'ms','yyyy-MM-dd')) as cnt
from ods.ecas_msg_log
;


set hivevar:compute_date=2020-06-18;




select
  msg_type,
  datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                                                            as create_time,
  unix_timestamp(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss'))                                            as create_time_unix,
  sha256(get_json_object(original_msg,'$.idNo'),'idNumber',1)                                                as id_no,
  get_json_object(original_msg,'$.creditId')                                                                 as credit_id,
  get_json_object(original_msg,'$.loanOrderId')                                                              as loan_order_id,
  cast(get_json_object(original_msg,'$.loanAmount')/100 as decimal(10,4))                                    as loan_amount,
  is_empty(get_json_object(original_msg,'$.totalInstallment'),0)                                             as loan_terms,
  get_json_object(original_msg,'$.loanUsage')                                                                as loan_usage,
  get_json_object(original_msg,'$.repayType')                                                                as repay_type,
  cast(is_empty(get_json_object(original_msg,'$.loanRating'),0)/1000000 as decimal(10,8))                    as loan_rating,
  cast(is_empty(get_json_object(original_msg,'$.penaltyInterestRate'),0)/1000000 as decimal(10,8))           as penalty_interest_rate,
  regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\',''),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}') as original_msg
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
  and get_json_object(original_msg,'$.loanOrderId') = 'DD000230362020060902050017e077'
;


select
  min(sync_date)
  -- min(create_time)
from ods_new_s.loan_info
where user_hash_no is null
limit 10
;

select distinct due_bill_no
from ods_new_s.loan_info
where user_hash_no is null and due_bill_no like 'DD%'
limit 10
;


select distinct *
from ods_new_s.customer_info
where user_hash_no = 'a_079a49407de852b365e18936f88b0bf2fd7b3f5e31c0dbdcba5ad83a6e2125ee'
;


select distinct *
from ods_new_s.loan_apply
where product_id in ()
due_bill_no = '1120061118351026472250'
   -- or user_hash_no = 'a_079a49407de852b365e18936f88b0bf2fd7b3f5e31c0dbdcba5ad83a6e2125ee'
limit 10
;



select distinct *
from ods.ecas_loan
where due_bill_no = '1120061515293978098957'
;

select distinct *
from ods.ecas_loan_asset
where due_bill_no = '1120061515293978098957'
;


select distinct
  acq_id,
  d_date,
  p_type
from ods.ecas_loan
where p_type = 'lx'
  and d_date is null
;


select
  max(d_date)
from ods.ecas_loan
where p_type = 'lx'
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-99-99')
;



select
  min(d_date)
from ods.ecas_loan
where 1 = 1
;



select
  biz_date,
  count(distinct due_bill_no) as cnt,
  count(due_bill_no) as apply_num,
  sum(loan_amount_apply) as amt_sum_apply,
  sum(loan_amount) as amt_sum
from ods_new_s.loan_apply
where product_id in ('001801','001802')
  and apply_status = 1
group by biz_date
order by biz_date
;



select distinct *
from ods_new_s.loan_apply
where product_id in ('001801','001802')
  and due_bill_no = '1120061118351026472250'
   -- or user_hash_no = 'a_079a49407de852b365e18936f88b0bf2fd7b3f5e31c0dbdcba5ad83a6e2125ee'
limit 10
;


set spark.executor.memoryOverhead=4g;
set spark.executor.memory=4g;
set hive.auto.convert.join=false;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.exec.max.dynamic.partitions=1500;

set hivevar:compute_date=2020-03-01;

insert overwrite table ods_new_s.loan_info partition(is_settled,product_id)
-- insert overwrite table ods_new_s.loan_info_bak partition(is_settled,product_id)
select *
-- from ods_new_s.loan_info
from ods_new_s.loan_info_bak
where 1 = 1
  and is_settled = 'no'
  -- and to_date(effective_time) <= date_add('${compute_date}',1)
  -- and (sync_date <= date_add('${compute_date}',1) or sync_date is null)
  -- and sync_date = to_date(effective_time)
  -- and sync_date = '${compute_date}'
-- limit 5
;



select distinct
  cust_id,
  age,
  due_bill_no,
  loan_active_date,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  is_settled,
  product_id
from ods_new_s.loan_info
-- from ods_new_s.loan_info_bak
-- from ods_new_s.loan_info_tmp
where is_settled = 'no'
  -- and (age is null or cust_id is null)
  and product_id is null
order by product_id,loan_active_date,due_bill_no,effective_time
-- limit 10
;






select distinct
  msg_type
from ods.ecas_msg_log
;


select distinct
  cust_id,
  age,
  due_bill_no,
  effective_time,
  expire_time,
  is_settled,
  product_id
from ods_new_s.loan_info
-- from ods_new_s.loan_info_tmp
-- where product_id in ('001701','001702')
where 1 = 1
  -- and due_bill_no like 'APP%'
  -- and due_bill_no = 'APP-20200527103659000007'
  and due_bill_no = '1120061423001155568783'
order by product_id,due_bill_no
-- limit 10
;



select distinct
  to_date(effective_time) as effective_time,
  product_id
from ods_new_s.loan_info_bak
order by effective_time desc
limit 10
;

select
  aa
from (
  select null as aa union all
  select 1 as aa union all
  select 20 as aa
) as tmp
where 1 = 1
  and (aa < 10 or aa is null)
;

select distinct
  *
from ods_new_s.loan_info
where 1 = 1
  -- and due_bill_no = 'DD000230362020062312200069197d'
  and due_bill_no in (
    -- '1000000325',
    -- '1000000275',
    -- '1000000145'
    'DD00023036202006301318009e2aae',
    'DD0002303620200701003700c3389b',
    'DD0002303620200701013900f45c09'
    -- 'DD0002303620200307225700e545de',
    -- 'DD0002303620200308222600ebbea9',
    -- 'DD00023036202003191421004c1251',
    -- 'DD0002303620200404090400770962',
    -- 'DD0002303620200409104700bfb018',
    -- 'DD0002303620200501162300e1b938'
  )
order by product_id,due_bill_no,loan_term,loan_term_repaid,effective_time
;



select distinct
  cust_id,
  user_hash_no,
  pre_apply_no,
  apply_id,
  due_bill_no,
  loan_apply_time,
  loan_amount_apply,
  loan_terms,
  loan_usage,
  loan_usage_cn,
  repay_type,
  repay_type_cn,
  interest_rate,
  penalty_rate,
  apply_status,
  apply_resut_msg,
  issue_time,
  loan_amount,
  create_time,
  biz_date,
  product_id
from ods_new_s.loan_apply
where 1 = 1
  -- and cust_id is null
  -- and biz_date = '2020-06-02'
  -- and product_id in ('001801','001802')
  and due_bill_no = '1120061423001155568783'
  -- and due_bill_no in (
  --   'DD00023036202006301318009e2aae',
  --   'DD0002303620200701003700c3389b',
  --   'DD0002303620200701013900f45c09'
    -- 'DD0002303620200307225700e545de',
    -- 'DD0002303620200308222600ebbea9',
    -- 'DD00023036202003191421004c1251',
    -- 'DD0002303620200404090400770962',
    -- 'DD0002303620200409104700bfb018',
    -- 'DD0002303620200501162300e1b938'
  -- )
;


select
  datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                         as create_time,
  datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                         as update_time,
  sha256(get_json_object(original_msg,'$.idNo'),'idNumber',1)             as id_no,
  get_json_object(original_msg,'$.loanOrderId')                           as loan_order_id,
  cast(get_json_object(original_msg,'$.loanAmount')/100 as decimal(10,4)) as loan_amount,
  is_empty(get_json_object(original_msg,'$.totalInstallment'),0)          as loan_terms,
  get_json_object(original_msg,'$.loanUsage')                             as loan_usage
  -- original_msg
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
  and original_msg is not null
  -- and get_json_object(original_msg,'$.loanOrderId') = 'DD000230362020062312200069197d'
  and get_json_object(original_msg,'$.loanOrderId') in (
    'DD000230362020062713350090ed89',
    'DD0002303620200628011300289c07'
    -- 'DD0002303620200307225700e545de',
    -- 'DD0002303620200308222600ebbea9',
    -- 'DD00023036202003191421004c1251',
    -- 'DD0002303620200404090400770962',
    -- 'DD0002303620200409104700bfb018',
    -- 'DD0002303620200501162300e1b938'
  )
limit 1
;



select distinct
  loan_apply.cust_id,
  loan_apply.user_hash_no,
  loan_apply.due_bill_no,
  age_birth(customer_info.birthday,to_date(loan_apply.issue_time)) as age,
  loan_apply.product_id
from (
  select distinct
    user_hash_no,
    issue_time,
    cust_id,
    due_bill_no,
    product_id
  from ods_new_s.loan_apply
  where 1 = 1
    and due_bill_no in (
      '1120061109235923622066',
      '1120061416135655454375',
      '1120061321015527872702',
      '1120061321380414085504',
      '1120061416332264597768',
      '1120061321051677104424',
      '1120061321090073942253',
      '1120061111582997914149',
      '1120061020464401568885',
      '1120061321173840667451'
      -- 'DD000230362020062312200069197d',
      -- 'DD0002303620200623124900b216c6',
      -- 'DD0002303620200623130800f274a8',
      -- 'DD000230362020062313400019c012',
      -- 'DD0002303620200623140000da43c3',
      -- 'DD0002303620200624005800b6c6a2',
      -- 'DD000230362020062401000025e8b0',
      -- 'DD00023036202006240115005f6a93',
      -- 'DD0002303620200624012900d6d9fe'
    )
) as loan_apply
left join (
  select distinct
    birthday,
    cust_id,
    product_id
  from ods_new_s.customer_info
) as customer_info
on loan_apply.product_id = customer_info.product_id and loan_apply.cust_id = customer_info.cust_id
;



select
  create_time,
  update_time,
  get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.applyNo') as due_bill_no
from (
  select
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
    -- regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\','') as original_msg
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\{','\{'),'\}\\\\\"','\}'),'\"\{','\{'),'\}\"','\}'),'\\\\','') as original_msg
  from ods.ecas_msg_log
  where msg_type = 'WIND_CONTROL_CREDIT'
    and original_msg is not null
) as tmp
where get_json_object(original_msg,'$.reqContent.jsonReq.content.reqData.applyNo') in (

      '1120061109235923622066',
      '1120061416135655454375',
      '1120061321015527872702',
      '1120061321380414085504',
      '1120061416332264597768',
      '1120061321051677104424',
      '1120061321090073942253',
      '1120061111582997914149',
      '1120061020464401568885',
      '1120061321173840667451'
)
;

select
  count(1)
from ods.ecas_msg_log
where msg_type = 'WIND_CONTROL_CREDIT'
  and original_msg is not null
  and datefmt(update_time,'ms','yyyy-MM-dd') = '2020-06-02'
;



select distinct
  user_hash_no,
  age,
  due_bill_no,
  effective_time,
  expire_time,
  product_id
from ods_new_s.loan_info
where 1 = 1
  and is_settled = 'no'
  -- and to_date(effective_time) <= date_add('2020-06-01',1)
  -- and due_bill_no in ('1120060216004289090275','1120060215213608230275')
  -- and (age is null or cust_id is null)
  and due_bill_no = 'APP-20200613162750000001'
limit 10
;

select distinct
  product_code                      as product_id,
  due_bill_no                       as due_bill_no,
  purpose                           as loan_usage,
  active_date                       as loan_active_date,
  cycle_day                         as cycle_day,
  loan_expire_date                  as loan_expire_date,
  loan_type                         as loan_type,
  case loan_type
  when 'R'    then '消费转分期'
  when 'C'    then '现金分期'
  when 'B'    then '账单分期'
  when 'P'    then 'POS分期'
  when 'M'    then '大额分期（专项分期）'
  when 'MCAT' then '随借随还'
  when 'MCEP' then '等额本金'
  when 'MCEI' then '等额本息'
  else loan_type
  end                               as loan_type_cn,
  loan_init_term                    as loan_init_term,
  curr_term                         as loan_term,
  repay_term                        as loan_term_repaid,
  remain_term                       as loan_term_remain,
  loan_status                       as loan_status,
  case loan_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else loan_status
  end                               as loan_status_cn,
  terminal_reason_cd                as loan_out_reason,
  loan_settle_reason                as paid_out_type,
  paid_out_date                     as paid_out_date,
  terminal_date                     as terminal_date,
  case
  when d_date = '2020-02-21' and sync_date    = 'ZhongHang'  then capital_plan_no
  when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
  when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
  when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
  else sync_date end                as sync_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')  as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as update_time,
  p_type,
  d_date
from ods.ecas_loan
where 1 = 1
  and d_date is not null
  -- and due_bill_no = '1120060216004289090275'
  -- and due_bill_no like 'APP%'
  and due_bill_no in (
    'DD000230362020062713350090ed89',
    'DD0002303620200628011300289c07'
    -- 'DD0002303620200307225700e545de',
    -- 'DD0002303620200308222600ebbea9',
    -- 'DD00023036202003191421004c1251',
    -- 'DD0002303620200404090400770962',
    -- 'DD0002303620200409104700bfb018',
    -- 'DD0002303620200501162300e1b938'
  )
order by loan_active_date,due_bill_no,d_date
;




select
  min(deal_date) as deal_date
from ods.nms_interface_resp_log
where sta_service_method_name = 'setupCustCredit'
  and standard_req_msg is not null
;

select distinct original_msg
from ods.ecas_msg_log
where msg_type = 'LOAN_APPLY'
  and original_msg is not null
limit 10
;


select age_birth('2020-06-24','2000-06-01') as a,age_birth('2000-06-01','2020-06-24') as b;


select distinct
  d_date
from ods.ecas_loan
order by d_date
;



select
  count(distinct due_bill_no) as due_bill_no,
  count(1) as total
from (
  select distinct
    product_code                      as product_id,
    loan_id                           as loan_id,
    due_bill_no                       as due_bill_no,
    contract_no                       as contract_no,
    apply_no                          as apply_no,
    purpose                           as loan_usage,
    register_date                     as register_date,
    request_time                      as request_time,
    active_date                       as loan_active_date,
    cycle_day                         as cycle_day,
    loan_expire_date                  as loan_expire_date,
    loan_type                         as loan_type,
    loan_init_term                    as loan_init_term,
    curr_term                         as loan_term,
    repay_term                        as loan_term_repaid,
    remain_term                       as loan_term_remain,
    loan_status                       as loan_status,
    terminal_reason_cd                as loan_out_reason,
    loan_settle_reason                as paid_out_type,
    paid_out_date                     as paid_out_date,
    terminal_date                     as terminal_date,
    loan_init_prin                    as loan_init_principal,
    interest_rate                     as loan_init_interest_rate,
    totle_int                         as loan_init_interest,
    term_fee_rate                     as loan_init_term_fee_rate,
    totle_term_fee                    as loan_init_term_fee,
    svc_fee_rate                      as loan_init_svc_fee_rate,
    totle_svc_fee                     as loan_init_svc_fee,
    penalty_rate                      as loan_init_penalty_rate,
    paid_principal                    as paid_principal,
    paid_interest                     as paid_interest,
    paid_penalty                      as paid_penalty,
    paid_svc_fee                      as paid_svc_fee,
    paid_term_fee                     as paid_term_fee,
    paid_mult                         as paid_mult,
    overdue_prin                      as overdue_principal,
    overdue_interest                  as overdue_interest,
    overdue_svc_fee                   as overdue_svc_fee,
    overdue_term_fee                  as overdue_term_fee,
    overdue_penalty                   as overdue_penalty,
    overdue_mult_amt                  as overdue_mult_amt,
    overdue_date                      as overdue_date,
    overdue_days                      as overdue_days,
    max_dpd                           as dpd_days_max,
    collect_out_date                  as collect_out_date,
    overdue_term                      as overdue_term,
    count_overdue_term                as overdue_terms_count,
    max_overdue_term                  as overdue_terms_max,
    max_overdue_prin                  as overdue_principal_max
  from ods.ecas_loan
  where 1 = 1
    and d_date != 'bak'
    and d_date is not null
    and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-09-09','9999-99-99')
) as ecas_loan
;


select
  count(distinct due_bill_no) as due_bill_no,
  count(1) as total
from (
  select distinct
    product_id,
    loan_id,
    due_bill_no,
    contract_no,
    apply_no,
    loan_usage,
    register_date,
    request_time,
    loan_active_date,
    cycle_day,
    loan_expire_date,
    loan_type,
    loan_init_term,
    loan_term,
    loan_term_repaid,
    loan_term_remain,
    loan_status,
    loan_out_reason,
    paid_out_type,
    paid_out_date,
    terminal_date,
    loan_init_principal,
    loan_init_interest_rate,
    loan_init_interest,
    loan_init_term_fee_rate,
    loan_init_term_fee,
    loan_init_svc_fee_rate,
    loan_init_svc_fee,
    loan_init_penalty_rate,
    paid_principal,
    paid_interest,
    paid_penalty,
    paid_svc_fee,
    paid_term_fee,
    paid_mult,
    overdue_principal,
    overdue_interest,
    overdue_svc_fee,
    overdue_term_fee,
    overdue_penalty,
    overdue_mult_amt,
    overdue_date,
    overdue_days,
    dpd_days_max,
    collect_out_date,
    overdue_term,
    overdue_terms_count,
    overdue_terms_max,
    overdue_principal_max
  from ods_new_s.loan_info
) as loan_info
;





select distinct
  due_bill_no,
  datefmt(lst_upd_time,'ms','yyyy-MM-dd') as update_time,
  d_date
from ods.ecas_loan
where d_date != 'bak'
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-09-09','9999-99-99')
  and d_date not between date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),-1) and date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),1)
limit 20
;

select distinct
  product_code                      as product_id,
  due_bill_no                       as due_bill_no,
  purpose                           as loan_usage,
  active_date                       as loan_active_date,
  cycle_day                         as cycle_day,
  case loan_type
  when 'R'    then '消费转分期'
  when 'C'    then '现金分期'
  when 'B'    then '账单分期'
  when 'P'    then 'POS分期'
  when 'M'    then '大额分期（专项分期）'
  when 'MCAT' then '随借随还'
  when 'MCEP' then '等额本金'
  when 'MCEI' then '等额本息'
  else loan_type
  end                               as loan_type_cn,
  loan_init_term                    as loan_init_term,
  curr_term                         as loan_term,
  repay_term                        as loan_term_repaid,
  remain_term                       as loan_term_remain,
  case loan_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else loan_status
  end                               as loan_status_cn,
  paid_out_date                     as paid_out_date,
  case
  when d_date = '2020-02-21' and sync_date    = 'ZhongHang'  then capital_plan_no
  when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
  when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
  when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
  else sync_date end                as sync_date,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as update_time,
  d_date
from ods.ecas_loan
where 1 = 1
  and d_date != 'bak'
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-09-09','9999-99-99')
  -- and d_date not between date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),-1) and date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),1)
  and due_bill_no = 'APP-20200527103659000007'
order by d_date
;


select distinct
  product_id,
  effective_time
-- from ods_new_s.loan_info
from ods_new_s.loan_info_tmp
order by effective_time desc
limit 10
;



set hivevar:compute_date=2020-06-27;

set spark.executor.memoryOverhead=4g;
set spark.executor.memory=4g;
set hive.auto.convert.join=false;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=500;
set hive.exec.max.dynamic.partitions=1500;

insert overwrite table ods_new_s.loan_info partition(is_settled,product_id)
select distinct *
from ods_new_s.loan_info
where 1 = 1
  and is_settled = 'no'
;








insert overwrite table ods_new_s.loan_info_bak partition(is_settled,product_id)
select *
from ods_new_s.loan_info
-- where 1 = 1
--   and due_bill_no in (
--     '1000000325',
--     '1000000275',
--     '1000000145'
--   )
-- limit 10
;

select distinct
  product_code                      as product_id,
  loan_id                           as loan_id,
  due_bill_no                       as due_bill_no,
  contract_no                       as contract_no,
  apply_no                          as apply_no,
  purpose                           as loan_usage,
  register_date                     as register_date,
  request_time                      as request_time,
  active_date                       as loan_active_date,
  cycle_day                         as cycle_day,
  loan_expire_date                  as loan_expire_date,
  loan_type                         as loan_type,
  case loan_type
  when 'R'    then '消费转分期'
  when 'C'    then '现金分期'
  when 'B'    then '账单分期'
  when 'P'    then 'POS分期'
  when 'M'    then '大额分期（专项分期）'
  when 'MCAT' then '随借随还'
  when 'MCEP' then '等额本金'
  when 'MCEI' then '等额本息'
  else loan_type
  end                               as loan_type_cn,
  loan_init_term                    as loan_init_term,
  curr_term                         as loan_term,
  repay_term                        as loan_term_repaid,
  remain_term                       as loan_term_remain,
  loan_status                       as loan_status,
  case loan_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else loan_status
  end                               as loan_status_cn,
  terminal_reason_cd                as loan_out_reason,
  loan_settle_reason                as paid_out_type,
  case loan_settle_reason
  when 'NORMAL_SETTLE'  then '正常结清'
  when 'OVERDUE_SETTLE' then '逾期结清'
  when 'PRE_SETTLE'     then '提前结清'
  when 'REFUND'         then '退车'
  when 'REDEMPTION'     then '赎回'
  else loan_settle_reason
  end                               as paid_out_type_cn,
  paid_out_date                     as paid_out_date,
  terminal_date                     as terminal_date,
  loan_init_prin                    as loan_init_principal,
  interest_rate                     as loan_init_interest_rate,
  totle_int                         as loan_init_interest,
  term_fee_rate                     as loan_init_term_fee_rate,
  totle_term_fee                    as loan_init_term_fee,
  svc_fee_rate                      as loan_init_svc_fee_rate,
  totle_svc_fee                     as loan_init_svc_fee,
  penalty_rate                      as loan_init_penalty_rate,
  paid_principal                    as paid_principal,
  paid_interest                     as paid_interest,
  paid_penalty                      as paid_penalty,
  paid_svc_fee                      as paid_svc_fee,
  paid_term_fee                     as paid_term_fee,
  paid_mult                         as paid_mult,
  overdue_prin                      as overdue_principal,
  overdue_interest                  as overdue_interest,
  overdue_svc_fee                   as overdue_svc_fee,
  overdue_term_fee                  as overdue_term_fee,
  overdue_penalty                   as overdue_penalty,
  overdue_mult_amt                  as overdue_mult_amt,
  overdue_date                      as overdue_date,
  overdue_days                      as overdue_days,
  max_dpd                           as dpd_days_max,
  collect_out_date                  as collect_out_date,
  overdue_term                      as overdue_term,
  count_overdue_term                as overdue_terms_count,
  max_overdue_term                  as overdue_terms_max,
  max_overdue_prin                  as overdue_principal_max,
  case
  when d_date = '2020-02-21' and sync_date = 'ZhongHang' then capital_plan_no
  when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
  when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
  when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
  else sync_date end                as sync_date,
  cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
  d_date                            as d_date
from ods.ecas_loan
where 1 = 1
  and d_date in ('2020-06-10','2020-06-11','2020-06-12')
  and loan_id in (
    '000015783984211admin000018000000',
    '000015849648221admin000068000041',
    '000015764976211admin003103000040'
  )
order by product_id,due_bill_no,loan_id,d_date,update_time
;


select *
from ods_new_s.loan_info
where 1 = 1
  and due_bill_no in (
    '1000000325',
    '1000000275',
    '1000000145'
  )
order by product_id,due_bill_no,loan_id,s_d_date,effective_time,expire_time
-- limit 10
;




select due_bill_no
from ods_new_s.loan_info
where 1 = 1
  and expire_time = '3000-12-31 00:00:00'
group by due_bill_no
having count(due_bill_no) > 1
limit 10
;





select distinct
  *
-- from ods_new_s.loan_info
-- from ods_new_s.loan_info_tmp
from ods_new_s.loan_info_bak
-- where product_id in ('001701','001702')
where 1 = 1
  -- and due_bill_no like 'APP%'
  -- and due_bill_no = 'APP-20200527103659000007'
  and due_bill_no = '1000000193'
order by product_id,due_bill_no,loan_term,loan_term_repaid,effective_time
  -- ,s_d_date
-- limit 10
;

select distinct
  product_code                      as product_id,
  due_bill_no                       as due_bill_no,
  purpose                           as loan_usage,
  active_date                       as loan_active_date,
  cycle_day                         as cycle_day,
  case loan_type
  when 'R'    then '消费转分期'
  when 'C'    then '现金分期'
  when 'B'    then '账单分期'
  when 'P'    then 'POS分期'
  when 'M'    then '大额分期（专项分期）'
  when 'MCAT' then '随借随还'
  when 'MCEP' then '等额本金'
  when 'MCEI' then '等额本息'
  else loan_type
  end                               as loan_type_cn,
  loan_init_term                    as loan_init_term,
  curr_term                         as loan_term,
  repay_term                        as loan_term_repaid,
  remain_term                       as loan_term_remain,
  case loan_status
  when 'N' then '正常'
  when 'O' then '逾期'
  when 'F' then '已还清'
  else loan_status
  end                               as loan_status_cn,
  paid_out_date                     as paid_out_date,
  overdue_days                      as overdue_days,
  case
  when d_date = '2020-02-21' and sync_date    = 'ZhongHang'  then capital_plan_no
  when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
  when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
  when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
  else sync_date end                as sync_date,
  cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as update_time,
  d_date
from ods.ecas_loan
where 1 = 1
  and d_date != 'bak'
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-09-09','9999-99-99')
  -- and d_date not between date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),-1) and date_add(datefmt(lst_upd_time,'ms','yyyy-MM-dd'),1)
  -- and due_bill_no = 'APP-20200527103659000007'
  and due_bill_no = '1000000193'
order by d_date
;




select distinct
  product_code as product_id,
  register_date,
  active_date
from ods.ecas_loan
where 1 = 1
  and d_date != 'bak'
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','2099-12-31','3030-06-05','9999-09-09','9999-99-99')
  -- and due_bill_no = 'APP-20200527103659000007'
  -- and due_bill_no = '1000000193'
  and register_date < active_date
order by product_id,register_date,active_date
limit 50
;



select
  msg_log_id,
  create_time,
  datefmt(get_json_object(original_msg,'$.timeStamp'),'ms','yyyy-MM-dd HH:mm:ss') as time_stamp,
  original_msg
from (
  select
    msg_log_id,
    -- original_msg
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    -- datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
    -- regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\\\\\\\\\"','\\\"'),'\\\\\"','\\\"'),'\\\\\\\\','\\\\') as original_msg
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\{','\{'),'\}\\\\\"','\}'),'\"\{','\{'),'\}\"','\}'),'\\\\\\\\\\\\\"','\"'),'\\\\\"','\"'),'\\\\\\\\','\\\\') as original_msg
  from ods.ecas_msg_log
  where 1 = 1
    and original_msg is not null
    and msg_type = 'GZ_CREDIT_APPLY'
    and msg_log_id = '000015923841001admin000077000007'
    -- and msg_type = 'GZ_CREDIT_RESULT'
    -- and msg_log_id = '000015923841001admin000027000003'
    -- and msg_type = 'GZ_LOAN_APPLY'
    -- and msg_log_id = '000015925551001admin000077000010'
    -- and msg_type = 'GZ_LOAN_RESULT'
    -- and msg_log_id = '000015925551001admin000027000012'
) as loan_result
where 1 = 1
  -- and get_json_object(original_msg,'$.data.applyNo') = 'APP-20200617165113000001'
  -- and get_json_object(original_msg,'$.reqContent.jsonReq.content.applyNo') = 'APP-20200617165113000001'
  -- and get_json_object(original_msg,'$.applyNo') = 'APP-20200617165113000001'
limit 1
;




select is_empty(cast(10 as string),cast(20 as string)) as tmp;

select is_empty(null);



select
  distinct
  jpa_version,
  out_side_schedule_no
  -- *
  -- count(1) as cnt
  -- max(d_date) as d_date_max,
  -- min(d_date) as d_date_min

  -- product_code         as product_id,
  -- due_bill_no          as due_bill_no,
  -- schedule_id          as schedule_id,
  -- out_side_schedule_no as out_side_schedule_no,
  -- loan_init_prin       as loan_init_principal,
  -- loan_init_term       as loan_init_term,
  -- curr_term            as loan_term,
  -- start_interest_date  as start_interest_date,
  -- pmt_due_date         as should_repay_date,
  -- origin_pmt_due_date  as should_repay_date_history,
  -- grace_date           as grace_date,
  -- due_term_prin        as should_repay_principal,
  -- due_term_int         as should_repay_interest,
  -- due_penalty          as should_repay_penalty,
  -- due_term_fee         as should_repay_term_fee,
  -- due_svc_fee          as should_repay_svc_fee,
  -- due_mult_amt         as should_repay_mult_amt,
  -- reduced_amt          as reduce_amount,
  -- reduce_term_prin     as reduce_principal,
  -- reduce_term_int      as reduce_interest,
  -- reduce_term_fee      as reduce_term_fee,
  -- reduce_svc_fee       as reduce_svc_fee,
  -- reduce_penalty       as reduce_penalty,
  -- reduce_mult_amt      as reduce_mult_amt
  ,d_date
from ods.ecas_repay_schedule
where 1 = 1
  and d_date is not null
  and d_date not in ('2025-06-02','2025-06-05','2025-06-06','9999-09-09','9999-99-99')
  and product_code = 'DIDI201908161538'
  and jpa_version is null
  and out_side_schedule_no is not null
  -- and due_bill_no = 'DD0002303620191011102400eafd91'
  -- and out_side_schedule_no = '3'
  -- and d_date in ('2020-02-21','2020-07-01')
  -- and d_date = '2020-02-27'
order by
  d_date
  -- ,schedule_id
-- limit 20
;



select
  -- distinct
  -- *
  count(1) as cnt
  -- max(s_d_date) as s_d_date

  -- product_id,
  -- due_bill_no,
  -- schedule_id,
  -- out_side_schedule_no,
  -- loan_init_principal,
  -- loan_init_term,
  -- loan_term,
  -- start_interest_date,
  -- should_repay_date,
  -- should_repay_date_history,
  -- grace_date,
  -- should_repay_principal,
  -- should_repay_interest,
  -- should_repay_penalty,
  -- should_repay_term_fee,
  -- should_repay_svc_fee,
  -- should_repay_mult_amt,
  -- reduce_amount,
  -- reduce_principal,
  -- reduce_interest,
  -- reduce_term_fee,
  -- reduce_svc_fee,
  -- reduce_penalty,
  -- reduce_mult_amt
from
-- ods_new_s.loan_info
ods_new_s.loan_info_tmp
-- ods_new_s.repay_schedule
-- ods_new_s.repay_schedule_tmp
-- ods_new_s.repay_schedule_bak
where 1 = 1
  and is_settled = 'no'
  -- and product_id = 'DIDI201908161538'
  -- and due_bill_no = 'DD0002303620191011102400eafd91'
-- distribute by product_id,loan_active_date,should_repay_date
-- order by schedule_id
-- limit 20
;




select
  *
  -- cast(date_add(overdue_date_start,cast(overdue_days as int) - 1) as string) as overdue_date,
  -- effective_time
from ods_new_s.loan_info
-- from ods_new_s.loan_info_bak
where 1 = 1
  and is_settled = 'no'
  and product_id = '001602'
  and due_bill_no = '1000000264'
order by effective_time
;

select is_empty('3.75',cast(1 as int)) as a;




select distinct
  product_id,
  case product_id
  when 'DIDI201908161538' then '滴滴 - 滴滴中航'
  when '001503'           then '盒子 - 盒伙人员工贷1'
  when '001504'           then '盒子 - 盒伙人员工贷2'
  when '001505'           then '盒子 - 盒子商户贷'
  when '001506'           then '凤金 - 凤凰金融1期'
  when '001507'           then '凤金 - 凤凰金融2期'
  when '001601'           then '汇通 - 汇通车分期-非LCV新车'
  when '001602'           then '汇通 - 汇通车分期-非LCV二手车'
  when '001603'           then '汇通 - 汇通车分期-LCV新车'
  when '001701'           then '瓜子 - 新车融资租赁'
  when '001702'           then '瓜子 - 二手车融资租赁'
  when '001801'           then '乐信 - 乐花卡'
  when '001802'           then '乐信 - 乐花借钱'
  else product_id end as product_name,
  loan_terms,
  repay_type_cn
from ods_new_s.loan_apply
order by product_id,loan_terms
;


select
  sum(a) as sm
from (
  select 1 as a union all
  select null as a
) as tmp
;

select
  count(distinct due_bill_no) as cnt
from mysql_json.ecasdb_ecas_loan
where 1 > 0
  and in_hive_type = 0
;

invalidate metadata ods.ecas_loan;
select * from ods.ecas_loan
where d_date = '2020-07-04'
limit 10;



select
  due_bill_no
from ods.ecas_loan
where 1 > 0
  and d_date in (${biz_date})
limit 1;


set hivevar:compute_date=2020-07-06;


invalidate metadata ods_new_s.loan_apply;
select distinct
  -- biz_date,
  apply_status,
  -- apply_resut_msg,
  product_id
from ods_new_s.loan_apply
order by product_id
;


select distinct
  get_json_object(original_msg,'$.status') as status
from ods.ecas_msg_log
where 1 > 0
  and msg_type='LOAN_RESULT'
;

select distinct
  sta_service_method_name,
  resp_code,
  resp_desc
from ods.nms_interface_resp_log
where 1> 0
  and sta_service_method_name = 'loanApply'
;

select
  -- datefmt(min(nvl(update_time,'1594108088000')),'ms','yyyy-MM-dd HH:mm:ss') as update_time
  -- min(nvl(update_time,'1594108088000'))
  is_empty(update_time,'1594108088000')
from ods.ecas_msg_log
where msg_type = 'GZ_CREDIT_APPLY'
;

select
  msg_type,
  datefmt(min(update_time),'ms','yyyy-MM-dd HH:mm:ss') as update_time
from ods.ecas_msg_log
group by msg_type
-- limit 1
;


explain
select
  count(1) as cnt
from ods_new_s.user_info
where to_date(update_time) = "${compute_date}"
;



explain
select distinct
  loan_apply.cust_id,
  loan_apply.user_hash_no,
  loan_apply.due_bill_no,
  age_birth(customer_info.birthday,to_date(loan_apply.issue_time)) as age,
  loan_apply.product_id
from (
  select distinct
    user_hash_no,
    issue_time,
    cust_id,
    due_bill_no,
    product_id
  from ods_new_s.loan_apply
  where 1 = 1
    and due_bill_no in (
      'DD000230362020062312200069197d',
      'DD0002303620200623124900b216c6',
      'DD0002303620200623130800f274a8',
      'DD000230362020062313400019c012',
      'DD0002303620200623140000da43c3',
      'DD0002303620200624005800b6c6a2',
      'DD000230362020062401000025e8b0',
      'DD00023036202006240115005f6a93',
      'DD0002303620200624012900d6d9fe'
    )
) as loan_apply
left join (
  select distinct
    birthday,
    cust_id as cust_id,
    cust_id as cust_id_1,
    product_id
  from ods_new_s.customer_info
) as customer_info
on loan_apply.product_id = customer_info.product_id and loan_apply.cust_id = customer_info.cust_id
;



-- 1000001011,1000000726 apply_status 为空
select distinct
  -- biz_date
  -- ,
  product_id
  -- ,due_bill_no
  ,apply_status
  -- ,apply_resut_msg
  -- ,loan_amount_apply
  -- ,loan_amount
from ods_new_s.loan_apply
where 1 > 0
  -- and apply_status is null
order by product_id,apply_status
;



select case
  when 1 = 1 then 1
  when 2 = 2 then 2
  else 3
  end as test
;



select
  map_tmp.a,
  map_tmp.b
from (
  select 1 as a,2 as b
) a
LATERAL VIEW explode (
  map(
    'a', a,
    'b', b
  )
) map_tmp as a,b
;





select size(map('a','','b',''));



select
  count(distinct due_bill_no) as due_bill_no_cnt,
  count(1) as cnt
from (
  select
    cust_id,
    user_hash_no,
    birthday,
    pre_apply_no,
    apply_id,
    due_bill_no,
    loan_apply_time,
    loan_amount_apply,
    loan_terms,
    loan_usage,
    loan_usage_cn,
    repay_type,
    repay_type_cn,
    interest_rate,
    penalty_rate,
    apply_status,
    apply_resut_msg,
    issue_time,
    loan_amount
  from ods_new_s.loan_apply
  where 1 > 0
    and biz_date = '2020-07-04'
  -- group by
  --   cust_id,
  --   user_hash_no,
  --   birthday,
  --   pre_apply_no,
  --   apply_id,
  --   due_bill_no,
  --   loan_apply_time,
  --   loan_amount_apply,
  --   loan_terms,
  --   loan_usage,
  --   loan_usage_cn,
  --   repay_type,
  --   repay_type_cn,
  --   interest_rate,
  --   penalty_rate,
  --   apply_status,
  --   apply_resut_msg,
  --   issue_time,
  --   loan_amount
  -- having count(due_bill_no) > 1
) as tmp
limit 10
;


select
  biz_date,
  due_bill_no
from ods_new_s.loan_apply
where 1 > 0
  -- and biz_date = '2020-07-04'
group by biz_date,due_bill_no
having count(due_bill_no) > 1
order by biz_date
limit 10
;



select
  cust_id,
  user_hash_no,
  birthday,
  pre_apply_no,
  apply_id,
  due_bill_no,
  loan_apply_time,
  loan_amount_apply,
  loan_terms,
  loan_usage,
  loan_usage_cn,
  repay_type,
  repay_type_cn,
  interest_rate,
  penalty_rate,
  apply_status,
  apply_resut_msg,
  issue_time,
  loan_amount,
  create_time,
  update_time,
  biz_date,
  product_id
from ods_new_s.loan_apply
where 1 > 0
  and due_bill_no = '1000000003'
;



select count(1) from ods_new_s.loan_apply;



ALTER TABLE ods_new_s.repay_detail DROP IF EXISTS partition (biz_date = '2020-07-08',product_id = '__HIVE_DEFAULT_PARTITION__');








select distinct
  user_hash_no,
  apply_id,
  biz_date as loan_apply_date,
  loan_amount_apply,
  loan_terms,
  apply_status,
  to_date(issue_time) as loan_approval_date,
  loan_amount,
  product_id
from ods_new_s.loan_apply
where 1 > 0
  -- and product_id = 'DIDI201908161538'
  and product_id in ('001601','001602','001603')
  and loan_amount_apply != 0
  -- and loan_approval_num_person != 0
limit 10
;




-- invalidate metadata
invalidate metadata
invalidate metadata ods_new_s.loan_info;
invalidate metadata ods_new_s.repay_detail;
invalidate metadata ods_new_s.repay_schedule;

select
  count(1) as cnt
from
ods_new_s.loan_info
-- ods_new_s.repay_schedule
-- ods_new_s.repay_schedule_tmp
-- ods_new_s.repay_schedule_bak
;



set hivevar:asset=_asset;
set hivevar:asset=;

select
  *
from ods.ecas_loan${asset}
limit 1
;




select distinct
  product_code                        as product_id,
  due_bill_no                         as due_bill_no,
  schedule_id                         as schedule_id,
  case
  when product_code = 'DIDI201908161538' and d_date = '2020-02-27' then null
  when product_code = 'DIDI201908161538' and d_date = '2020-02-28' then null
  when product_code = 'DIDI201908161538' and d_date = '2020-02-29' then null
  when product_code = 'DIDI201908161538' and d_date = '2020-03-01' then null
  else out_side_schedule_no end       as out_side_schedule_no,
  loan_init_prin                      as loan_init_principal,
  loan_init_term                      as loan_init_term,
  curr_term                           as loan_term,
  start_interest_date                 as start_interest_date,
  pmt_due_date                        as should_repay_date,
  origin_pmt_due_date                 as should_repay_date_history,
  grace_date                          as grace_date,
  due_term_prin                       as should_repay_principal,
  due_term_int                        as should_repay_interest,
  due_penalty                         as should_repay_penalty,
  due_term_fee                        as should_repay_term_fee,
  due_svc_fee                         as should_repay_svc_fee,
  due_mult_amt                        as should_repay_mult_amt,
  reduced_amt                         as reduce_amount,
  reduce_term_prin                    as reduce_principal,
  reduce_term_int                     as reduce_interest,
  reduce_term_fee                     as reduce_term_fee,
  reduce_svc_fee                      as reduce_svc_fee,
  reduce_penalty                      as reduce_penalty,
  reduce_mult_amt                     as reduce_mult_amt,
  is_empty(create_time,create_user)   as create_time,
  is_empty(lst_upd_time,lst_upd_user) as update_time,
  t_d_date
  -- d_date                              as d_date,
  -- lag(d_date) over(order by d_date)   as lag_d_date
from ods.ecas_repay_schedule
join (
  select
    schedule_id as schedule_id_tmp,
    max(d_date) as t_d_date
  from ods.ecas_repay_schedule
  where 1 > 0
    and d_date < '2020-03-01'
  group by schedule_id
) as ecas_repay_schedule_tmp
on ecas_repay_schedule.schedule_id = ecas_repay_schedule_tmp.schedule_id_tmp
where 1 > 0
  -- and d_date is not null
  -- and d_date not in ('bak','2025-06-02','2025-06-05','2025-06-06','9999-09-09','9999-99-99')
  and schedule_id = '000015758928301admin003103000031'
;


select
  -- due_bill_no,
  schedule_id,
  -- d_date
  max(d_date)
  -- ,lag(d_date) over(partition by schedule_id order by d_date) as lag_d_date
from ods.ecas_repay_schedule
where 1 > 0
  -- and d_date is not null
  -- and d_date not in ('bak','2025-06-02','2025-06-05','2025-06-06','9999-09-09','9999-99-99')
  and d_date < '2020-03-02'
  -- and due_bill_no = '1000000054'
  -- and schedule_id = '000015758928301admin003103000031'
group by
  -- d_date,
  -- due_bill_no,
  schedule_id
order by
  -- due_bill_no,
  schedule_id
  -- d_date desc
limit 100
;


select
  schedule_id,
  max(d_date)
from ods.ecas_repay_schedule
where 1 > 0
  and d_date < '2020-03-02'
group by schedule_id
;



select
  count(1) as cnt
from ods_new_s.repay_schedule_bak



select
  distinct
  -- product_id
  apply_status,
  apply_resut_msg
  -- ,
  -- count(nvl(loan_amount,'a')) as loan_amount
  -- *
  -- case
  -- when nvl(loan_amount,0) != 0 then 1
  -- when apply_status = 1 then 4
  -- when apply_status = 2 then 5
  -- else 2 end as apply_status,
  -- case
  -- when nvl(loan_amount,0) != 0 then '放款成功'
  -- when apply_status = 1 then '用信通过'
  -- when apply_status = 2 then '用信失败'
  -- else '放款失败' end as apply_resut_msg
from
ods_new_s.loan_apply
-- ods_new_s.loan_apply_tmp
where 1 > 0
  and product_id in ('001801','001802')
  -- and apply_status is null
  -- and loan_amount = 0
  -- and loan_amount is null
;

invalidate metadata ods_new_s.loan_apply;
invalidate metadata ods_new_s.loan_apply_tmp;

insert overwrite table ods_new_s.loan_apply_tmp partition(biz_date,product_id)
-- select distinct
--   apply_status,
--   apply_resut_msg
-- from (
select
  cust_id,
  user_hash_no,
  birthday,
  pre_apply_no,
  apply_id,
  due_bill_no,
  loan_apply_time,
  loan_amount_apply,
  loan_terms,
  loan_usage,
  loan_usage_cn,
  repay_type,
  repay_type_cn,
  interest_rate,
  penalty_rate,
  case
  when nvl(loan_amount,0) != 0 then 1
  when apply_status = 1 then 4
  when apply_status = 2 then 5
  else 2 end as apply_status,
  case
  when nvl(loan_amount,0) != 0 then '放款成功'
  when apply_status = 1 then '用信通过'
  when apply_status = 2 then '用信失败'
  else '放款失败' end as apply_resut_msg,
  issue_time,
  loan_amount,
  risk_level,
  risk_score,
  ori_request,
  ori_response,
  create_time,
  update_time,
  biz_date,
  product_id
from
ods_new_s.loan_apply
-- ods_new_s.loan_apply_tmp
where 1 > 0
  and product_id in ('001801','001802')
-- limit 10
-- ) as tmp
;

insert overwrite table ods_new_s.loan_apply partition(biz_date,product_id)
select * from ods_new_s.loan_apply_tmp;


select size(map('age',1));



drop table test_map;
create table test_map(
  ts string,
  gx map<string,int> comment '测试'
)
STORED AS PARQUET;

insert into table test_map
select 'a',map('age',1) union all
select 'a',map('age',15) union all
select 'a',map('age',20) union all
select 'a',map('age',21)
;

drop table test_map1;
create table test_map1(
  ts string,
  gx string comment '测试'
)
STORED AS PARQUET;

insert into table test_map1
select * from test_map;

invalidate metadata test_map;
select size(gx) from test_map;





select
  distinct

  -- payment_id,
  -- due_bill_no,
  -- acct_nbr,
  -- acct_type,
  -- bnp_type,
  -- repay_amt,
  -- batch_date,
  -- create_time,
  -- create_user,
  -- lst_upd_time,
  -- lst_upd_user,
  -- jpa_version,
  -- term,
  -- org,
  -- order_id,
  -- txn_seq,
  -- txn_date,
  -- overdue_days,
  -- loan_status
  biz_date,
  sum(repay_amount) over(partition by biz_date) as repay_amount
  -- due_bill_no,
  -- loan_status_cn,
  -- overdue_days,
  -- biz_date,
  -- product_id
from
ods_new_s.repay_detail
-- ods.ecas_repay_hst
where 1 > 0
  -- and due_bill_no in (
  --   '1120061017361786522786',
  --   '1120061211013462742786'
  -- )
  -- and overdue_days > 0
  and product_id in ('001801','001802')
  -- and loan_status in ('F','O','N')
-- limit 10
;



select
  biz_date,
  loan_status_cn,
  sum(repay_amount) as repay_amount
from
ods_new_s.repay_detail
where 1 > 0
  and product_id in ('001801','001802')
group by biz_date,loan_status_cn
order by biz_date,loan_status_cn
;




select
  distinct
  *
  -- schedule_id,
  -- max(d_date)
from
ods_new_s.repay_schedule
where 1 > 0
  -- and d_date < '2020-03-02'
  and due_bill_no in (
    '1120061017361786522786',
    '1120061211013462742786'
  )
-- group by schedule_id
;


select
  distinct
  *
from
ods_new_s.loan_info
where 1 > 0
  -- and d_date < '2020-03-02'
  and due_bill_no in (
    '1120061017361786522786',
    '1120061211013462742786'
  )
order by due_bill_no,s_d_date
;





invalidate metadata ods.ecas_order;

-- select
--   count(1) as cnt
-- from (
select
  distinct
  order_id,
  apply_no,
  due_bill_no,
  ori_order_id,
  term,
  channel_id,
  pay_channel,
  command_type,
  order_status,
  datefmt(cast(order_time as string),'ms','yyyy-MM-dd HH:mm:ss') as order_time,
  repay_serial_no,
  service_id,
  assign_repay_ind,
  repay_way,
  txn_type,
  txn_amt,
  original_txn_amt,
  success_amt,
  currency,
  code,
  message,
  response_code,
  response_message,
  business_date,
  send_time,
  datefmt(cast(opt_datetime as string),'ms','yyyy-MM-dd HH:mm:ss') as opt_datetime,
  setup_date,
  loan_usage,
  purpose,
  online_flag,
  online_allow,
  order_pay_no,
  bank_trade_no,
  bank_trade_time,
  bank_trade_act_no,
  bank_trade_act_name,
  service_sn,
  outer_no,
  confirm_flag,
  datefmt(cast(txn_time as string),'ms','yyyy-MM-dd HH:mm:ss') as txn_time,
  txn_date,
  bank_trade_act_phone,
  capital_plan_no,
  memo,
  nvl(datefmt(cast(create_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as create_time,
  nvl(datefmt(cast(lst_upd_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as lst_upd_time,
  d_date
from ods.ecas_order
where 1 > 0
  and d_date is not null
  and d_date not in ('bak','2025-06-05','2025-06-06','2028-06-14','2030-06-05','2030-06-06','3030-06-05','3030-06-06','9999-09-09','9999-99-99')
  -- and d_date in ('2020-01-24','2020-02-01','2020-02-03','2020-02-07')
  -- and to_date(nvl(datefmt(cast(lst_upd_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp))) = ''
  -- and ori_order_id is not null
  and datefmt(cast(txn_time as string),'ms','yyyy-MM-dd') != txn_date
  -- and apply_no != bank_trade_no
  -- and setup_date != datefmt(create_time,'ms','yyyy-MM-dd')
  -- and order_time is null
  -- and create_time is null
  -- and txn_date is null
  -- and confirm_flag is null
  -- and service_sn is null
-- txn_time
limit 10
-- ) as tmp
;


invalidate metadata ods_new_s.order_info;
select
  *
from ods_new_s.order_info
where 1 > 0
  and biz_date in ('2020-01-24','2020-02-01','2020-02-03','2020-02-07')
limit 10;


-- datefmt(order_time,'ms','yyyy-MM-dd HH:mm:ss') as order_time,
-- datefmt(opt_datetime,'ms','yyyy-MM-dd HH:mm:ss') as opt_datetime,
-- datefmt(txn_time,'ms','yyyy-MM-dd HH:mm:ss') as txn_time,
-- datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
-- datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as lst_upd_time,





-- 1150847
select
  issue_date,
  count(due_bill_no) as cnt,
  sum(loan_amount) as loan_amount
from (
  select distinct
    due_bill_no,
    to_date(issue_time) as issue_date,
    loan_amount
  from ods_new_s.loan_apply
  where 1  > 0
    and apply_status = 1
    -- and product_id = 'DIDI201908161538'
    and product_id = '001801'
) as tmp
group by issue_date
order by issue_date
;

-- 1152153
select
  product_id,
  loan_active_date         as loan_active_date,
  count(due_bill_no)       as loan_num,
  sum(loan_init_principal) as loan_amount
from (
  select distinct
    product_id,
    loan_active_date,
    due_bill_no,
    loan_init_principal
  from ods_new_s.loan_info
  where 1 > 0
    -- and product_id = 'DIDI201908161538'
    and product_id = '001801'
    -- and product_id = '001802'
    -- and product_id in ('001801','001802')
) as loan_active_num
group by loan_active_date,product_id
order by loan_active_date,product_id
;




-- 1399027

-- DIDI201908161538  46084
-- 001601            173
-- 001602            232
-- 001603            566
-- 001702            9
-- 001801            1228909
-- 001802            123054

-- DIDI201908161538  9928
-- 001601            139
-- 001602            195
-- 001603            480
-- 001702            7
-- 001801            1150847
-- 001802            110030
select
  product_id,
  count(distinct due_bill_no) as cnt
from ods_new_s.loan_apply
where 1  > 0
  and apply_status = 1
group by product_id
order by product_id
;


-- 1273114

-- DIDI201908161538  9933
-- 001601            139
-- 001602            195
-- 001603            491
-- 001702            11
-- 001801            1152153
-- 001802            110192
select
  product_id,
  count(distinct due_bill_no) as cnt
from ods_new_s.loan_info
group by product_id
;



select least('2020-07-14',to_date('2020-07-01 10:10:10')) as min,greatest('2020-07-14','2020-07-01 10:10:10') as max;
select min(array("2020-07-14 21:35:01",null));
select
  a
  -- min(a)
from (
  select array("2020-07-14 21:35:01",null) as a
  -- select 'zzz' as a union all
  -- select 'aaa' as a
) as tmp
;

if(get_json_object(loan_apply.original_msg,'$.reqContent.reqMsgCreateDate') > loan_apply.create_time,loan_apply.create_time,loan_apply.create_time)
set hivevar:compute_date=2020-07-06;
-- set hivevar:compute_date=2020-06-17;
  select distinct
    least(create_date,datefmt(get_json_object(original_msg,'$.timeStamp'),'ms','yyyy-MM-dd')) as biz_date,
  from (
    select
      datefmt(create_time,'ms','yyyy-MM-dd') as create_date,
      -- regexp_replace(
      --   regexp_replace(
      --     regexp_replace(
      --       original_msg,'\\\"\\\{','\\\{'
      --     ),'\\\}\\\"','\\\}'
      --   ),'\\\\\"','\\\"'
      -- ) as original_msg
      regexp_replace(
        regexp_replace(
          regexp_replace(
            original_msg,'\"\{','\{'
          ),'\}\"','\}'
        ),'\\\\\"','\"'
      ) as original_msg
    from ods.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_LOAN_APPLY'
      and original_msg is not null
  ) as tmp
order by create_date
;




invalidate metadata ods_new_s.loan_apply;
select
  count(distinct due_bill_no) as due_bill_no,
  count(1) as cnt
from ods_new_s.loan_apply
;

invalidate metadata ods_new_s.order_info;
select
  count(distinct due_bill_no) as due_bill_no,
  count(1) as cnt
from ods_new_s.order_info
;




select
  case loan_active_num.product_id
  when '001601'           then '0016'
  when '001602'           then '0016'
  when '001603'           then '0016'
  when '001702'           then '0017'
  when '001801'           then '0018'
  when '001802'           then '0018'
  when 'DIDI201908161538' then 'DIDI'
  else loan_active_num.product_id end      as product_id,
  case loan_active_num.product_id
  when '001601'           then '汇通'
  when '001602'           then '汇通'
  when '001603'           then '汇通'
  when '001702'           then '瓜子'
  when '001801'           then '乐信'
  when '001802'           then '乐信'
  when 'DIDI201908161538' then '滴滴'
  else loan_active_num.product_id end      as product_name,
  count(loan_active_num.due_bill_no)       as loan_num,
  sum(loan_active_num.loan_init_principal) as loan_amount,
  avg(loan_active_num.loan_init_principal) as avg_loan_amount
from (
  select distinct
    product_id,
    loan_init_term,
    loan_init_principal,
    due_bill_no
  from ods_new_s.loan_info
  where 1 > 0
    -- and
) as loan_active_num
group by
  case loan_active_num.product_id
  when '001601'           then '0016'
  when '001602'           then '0016'
  when '001603'           then '0016'
  when '001702'           then '0017'
  when '001801'           then '0018'
  when '001802'           then '0018'
  when 'DIDI201908161538' then 'DIDI'
  else loan_active_num.product_id end,
  case loan_active_num.product_id
  when '001601'           then '汇通'
  when '001602'           then '汇通'
  when '001603'           then '汇通'
  when '001702'           then '瓜子'
  when '001801'           then '乐信'
  when '001802'           then '乐信'
  when 'DIDI201908161538' then '滴滴'
  else loan_active_num.product_id end
order by product_id
;



invalidate metadata ods_new_s.credit_apply;
select distinct
  product_id,
  resp_code,
  resp_msg
from ods_new_s.credit_apply
order by product_id,resp_code
;


invalidate metadata ods_new_s.loan_apply;
select distinct
  product_id,
  apply_status,
  apply_resut_msg
from ods_new_s.loan_apply
order by product_id,apply_status
;




select
  distinct
  -- create_time,
  -- update_time
  ret_msg,
  to_date(create_time) as create_time,
  datefmt(update_time,'ms','yyyy-MM-dd') as update_time
from ods.t_personas
where 1 > 0
  and to_date(create_time) != datefmt(update_time,'ms','yyyy-MM-dd')
-- limit 1
;



select
  *
  -- due_bill_no,
  -- overdue_days
from ods_new_s.loan_info
where 1 > 0
  -- and overdue_days > 100
  and due_bill_no = '1000000279'
  and (s_d_date <= '2020-06-01' or '2020-06-30' < e_d_date)
order by s_d_date
-- limit 1
;




select distinct
  current_risk_control_status
from dm.dm_watch_bill_snapshot
;









drop table test_map;
create table test_map(
  id string,
  msg string
);



with base as (
  select '1' as a,concat('{',concat_ws(',',array('"age":1','"age":60')),'}') as b union all
  select '2' as a,concat('{',concat_ws(',',array('"age":15')),'}')           as b union all
  select '1' as a,concat('{',concat_ws(',',array('"age":20')),'}')           as b union all
  select '2' as a,concat('{',concat_ws(',',array('"age":18')),'}')           as b union all
  select '1' as a,concat('[',concat_ws(',',array('"a"',1)),']')              as b
)
from base
insert overwrite table test_map select * where a = 1
insert into table test_map select * where a = 2
;


invalidate metadata test_map;
select id,msg from test_map;

set hive.support.quoted.identifiers=None;
select `(id)?+.+` from test_map;


select concat(map('aa',1)) test_map;
select concat(array('aa',1)) test_map;



select date_sub('2020-07-27',3);


select null + 1;


invalidate metadata ods_new_s.loan_apply;
select distinct
  to_date(loan_apply_time) as loan_apply_date,
  product_id,
  apply_status,
  to_date(issue_time) as issue_date
from ods_new_s.loan_apply
order by loan_apply_date,product_id,issue_date
;




select
  min(should_repay_date) as should_repay_date,
  product_id
from ods_new_s.repay_schedule
group by product_id
order by should_repay_date,product_id
;

select
  min(loan_active_date) as loan_active_date,
  product_id
from ods_new_s.loan_info
group by product_id
order by loan_active_date,product_id
;



select
  *
  -- due_bill_no,
  -- s_d_date,
  -- overdue_date_start,
  -- overdue_days,
  -- overdue_date,
  -- product_id
from ods_new_s.loan_info
where 1 > 0
  -- and product_id in ('001801','001802')
  -- and overdue_days > 0
  -- and due_bill_no = 'DD0002303620191023124800eb526b'
  and due_bill_no = '1000000204'
order by s_d_date,overdue_days
-- limit 20
;


select
  product_id,
  sort_array(collect_set(overdue_days)) as overdue_days
from ods_new_s.loan_info
where 1 > 0
  -- and product_id in ('001801','001802')
  -- and overdue_days > 0
  and due_bill_no = 'DD0002303620191023124800eb526b'
  -- and due_bill_no = '1000000204'
group by product_id
-- limit 20
;



set hive.execution.engine=spark;
select
  concat('[',concat_ws(',',arr),']')
from (
  select array('a','b') as arr union all
  select array('1','2') as arr
) as tmp
;



invalidate metadata dw_new.dw_loan_base_stat_loan_num_day;
select
  *
  -- count(1)
from dw_new.dw_loan_base_stat_loan_num_day
where 1 > 0
  -- and biz_date <= '2019-11-13'
  and biz_date = '2020-06-16'
order by biz_date,product_id,loan_terms,loan_num
;



invalidate metadata ods_new_s.repay_detail;
select
  distinct
  -- *
  biz_date,
  product_id
from ods_new_s.repay_detail
where 1 > 0
  and product_id is null
  -- and order_id = '000015958437001admin000077000009'
  -- and payment_id = '000015958437001admin000077000010'
;


select
  *
from ods.ecas_loan
where 1 > 0
  and product_code in ('001801','001802')
  and overdue_days > 0
limit 10
;

set hive.execution.engine=spark;
set hive.execution.engine=mr;


select month('2020-07-28') - month('2020-06-28');

set hivevar:ST9=2020-07-01;



select
  *
from dw_new.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and product_id in ('001801','001802')
  and overdue_days > 0
limit 10
;


select
  *
from dw_new.dw_loan_base_stat_should_repay_day
where 1 > 0
  and biz_date in ('2020-06-02','2020-06-03','2020-06-04')
  and product_id in ('001801','001802')
limit 10
;

select
  -- *
  min(should_repay_date) as should_repay_date
from ods_new_s.repay_schedule
where 1 > 0
  and product_id in ('001801','001802')
  -- and should_repay_date in ('2020-06-02','2020-06-03','2020-06-04')
limit 10
;


select
  -- *
  loan_init_term,
  count(distinct due_bill_no) as loan_num,
  sum(loan_init_principal) as loan_init_principal
from ods_new_s.loan_info
where 1 > 0
  and product_id in ('001801','001802')
  and loan_active_date = '2020-07-28'
group by loan_init_term
limit 10
;

set VAR:db_suffix=_cps;

-- invalidate metadata dw_new${VAR:db_suffix}.dw_loan_base_stat_loan_num_day;
select
  *
from dw_new${VAR:db_suffix}.dw_loan_base_stat_loan_num_day
where 1 > 0
  and product_id in ('001801','001802')
  -- and biz_date = '2020-07-28'
  and biz_date = '2020-06-03'
order by product_id,loan_terms,loan_num
-- limit 10
;


invalidate metadata dw_new${VAR:db_suffix}.dw_loan_base_stat_repay_detail_day;
select
  *
from dw_new${VAR:db_suffix}.dw_loan_base_stat_repay_detail_day
where 1 > 0
  and product_id in ('001801','001802')
  and biz_date = '2020-06-02'
  -- and biz_date = '2020-07-28'
order by product_id,loan_terms
-- limit 10
;



select
  max(biz_date)
from ods_new_s_cps.repay_detail
;


select
  *
from dw_new.dw_loan_apply_stat_day
where 1 > 0
  -- and biz_date = '2020-06-09'
  and biz_date = '2020-06-05'
  and product_id in ('001801','001802')
order by product_id,loan_terms
;


invalidate metadata dw_new.dw_loan_ret_msg_day;
select
  *
from dw_new.dw_loan_ret_msg_day
where 1 > 0
  and biz_date = '2020-07-28'
  and product_id in ('001801','001802')
order by product_id,loan_terms,ret_msg
;


invalidate metadata ods_new_s.loan_apply;
select
  -- due_bill_no,
  sum(loan_amount)          as loan_amount,
  sum(loan_amount_approval) as loan_amount_approval
  -- max(datediff(to_date(loan_apply_time),to_date(issue_time))) as tt
  -- loan_amount
from ods_new_s.loan_apply
where 1 > 0
  and biz_date = '2020-06-05'
  -- and loan_approval_num_sum = loan_approval_num_count
  and loan_terms = 1
  -- and loan_amount is null
  -- and product_id in ('001801','001802')
  and product_id = '001801'
-- limit 10
;


select
  *
from
ods.ecas_loan
-- ods.ecas_repay_schedule
where 1 > 0
  and p_type = 'lx'
  -- and product_code = '001801'
  and due_bill_no in (
    '1120060511120607576033',
    '1120060510293537455084',
    '1120060510294109809437'
  )
limit 10
;



set var:db_name=
ods_new_s.repay_schedule
-- dw_new.dw_loan_approval_stat_day
-- dw_new.dw_loan_apply_stat_day
;

invalidate metadata ${VAR:db_name};

select
  -- *
  due_bill_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  loan_term,
  should_repay_date,
  -- should_repay_amount,
  should_repay_principal,
  -- should_repay_interest,
  -- should_repay_penalty,
  -- should_repay_term_fee,
  -- should_repay_svc_fee,
  -- should_repay_mult_amt,
  s_d_date,
  e_d_date,
  product_id
from ${VAR:db_name}
where 1 > 0
  -- and biz_date = '2020-06-02'
  -- and biz_date in ('2020-06-05','2020-06-07','2020-06-08')
  -- and loan_approval_num_sum = loan_approval_num_count
  -- and loan_terms = 1
  and product_id in ('001801','001802')
  and due_bill_no = '1120070114545200768685'
  -- and product_id = '001801'
-- order by biz_date,product_id,loan_terms
order by product_id,loan_init_term,loan_term,s_d_date
-- limit 10
;


select distinct
  paid_out_type,
  case paid_out_type
  when 'NORMAL_SETTLE'  then '正常结清'
  when 'OVERDUE_SETTLE' then '逾期结清'
  when 'PRE_SETTLE'     then '提前结清'
  when 'REFUND'         then '退车'
  when 'REDEMPTION'     then '赎回'
  when 'BANK_REF'       then '放款退票'
  else paid_out_type
  end as paid_out_type_cn
from ods.ecas_repay_schedule
;





invalidate metadata dw_new.dw_loan_base_stat_repay_detail_day;
select
  *
from dw_new.dw_loan_base_stat_repay_detail_day
where 1 > 0
  and biz_date = '2020-07-28'
  -- and loan_approval_num_sum = loan_approval_num_count
  -- and loan_terms = 1
  and product_id in ('001801','001802')
  -- and product_id = '001801'
order by biz_date,product_id,loan_terms
-- limit 10
;




invalidate metadata dw_new.dw_credit_apply_stat_day;

set hivevar:ST9=2020-07-01;
set hivevar:db_suffix=_cps;
set hivevar:db_suffix=;



select
  a,
  case
  when 2 < a then 3
  when 1 < a then 2
  when 0 < a then 1
  else 0 end
from (
  select 1 as a union all
  select 2 as a union all
  select 3 as a union all
  select 4 as a
) as tmp
;





select
  distinct
  resp_code,
  apply_amount,
  credit_amount
  -- biz_date,
  -- sum(apply_amount),
  -- sum(credit_amount)
from ods_new_s.credit_apply
where 1 > 0
  and resp_code = '2'
  -- and biz_date between date_sub('2020-06-15',1) and '2020-06-15'
-- group by biz_date
;


select distinct
  paid_out_type
  -- s_d_date
  -- overdue_date
  -- product_id,
  -- due_bill_no,
  -- loan_status_cn,
  -- overdue_principal,
  -- overdue_interest
from
-- ods_new_s_cps.loan_info
ods.ecas_repay_schedule
where 1 > 0
  -- and product_id in ('001601','001602','001603')
-- order by
-- overdue_date
-- s_d_date
-- limit 10
;

select nvl(12/0,0)*100;






select
  biz_date,
  count(1)
from dm_eagle.eagle_loan_info
group by biz_date
order by biz_date
;





set hivevar:table=
-- dm_eagle.eagle_loan_info
-- dm_eagle.eagle_repay_schedule
-- dm_eagle.eagle_repay_detail
dm_eagle.eagle_repay_detail_partition
-- dm_eagle_cps.eagle_loan_info
-- dm_eagle_cps.eagle_repay_schedule
-- dm_eagle_cps.eagle_repay_detail
;
msck repair table ${table};
select * from ${table} limit 10;



select *
from dw_new.dw_loan_apply_stat_day
where 1 > 0
  and product_id in ('001801','001802')
limit 10
;


select
  `(is_settled|product_id)?+.+`
from ods_new_s.loan_info
where 1 > 0
  and due_bill_no = '1120070611473712088686'
;



select *
from
-- dm_eagle.eagle_migration_rate_month
dw_new.dw_loan_base_stat_overdue_num_day
limit 10
;




select *
from ods_new_s.loan_info
where 1 > 0
  and product_id = '001801'
  -- and s_d_date <= '2020-07-06' and '2020-07-06' < e_d_date
  -- and loan_init_term = 1
  -- and loan_active_date = '2020-06-06'
  and due_bill_no = '1120060420501004691039'
  -- and overdue_date_first = '2020-06-17'
limit 10
;

select *
from
-- ods.ecas_repay_schedule
ods_new_s.repay_schedule
where 1 > 0
  and product_id = '001801'
  -- and product_code = '001801'
  -- and due_bill_no in (
  --   '1120060602301006990582'
  --   -- '1120060603005467686032',
  --   -- '1120060603080555106032',
  --   -- '1120060602241148366852',
  --   -- '1120060603072020996032'
  -- )
order by due_bill_no
limit 10
;





select distinct
  loan_info.due_bill_no,
  loan_init_term,
  paid_out_type
from (
  select due_bill_no,loan_init_term
  from dm_eagle_cps.eagle_loan_info
  where 1 > 0
    and product_id in ('001801','001802')
    and loan_init_term = 9
) as loan_info
join (
  select due_bill_no,paid_out_type
  from dm_eagle_cps.eagle_repay_schedule
  where 1 > 0
    and product_id in ('001801','001802')
    and paid_out_type is not null
) as repay_schedule
on loan_info.due_bill_no = repay_schedule.due_bill_no
join (
  select due_bill_no
  from dm_eagle_cps.eagle_repay_detail
  where 1 > 0
    and product_id in ('001801','001802')
) as repay_detail
on loan_info.due_bill_no = repay_detail.due_bill_no
order by due_bill_no
-- limit 10
;



select
  count(1) as cnt
  -- *
from
ods_new_s_cps.repay_schedule
-- ods_new_s_cps.repay_schedule_tmp
where 1 > 0
  and product_id in ('001801','001802')
  -- and e_d_date = '3000-12-31'
  -- and should_repay_date between date_sub('${ST9}',30) and '${ST9}'
  -- and due_bill_no = '1120061514363028353120'
;




insert overwrite table ods_new_s.customer_info partition(product_id)
select * from dm_eagle.customer_info
;



set hivevar:ST9=2020-10-06;

select distinct
  due_bill_no       as due_bill_no_repay_schedule,
  loan_init_term    as loan_init_term_repay_schedule,
  loan_term         as loan_term_repay_schedule,
  should_repay_date as should_repay_date,
  product_id        as product_id_repay_schedule
from ods_new_s${db_suffix}.repay_schedule
where 1 > 0
  and product_id in ('001801','001802')
    and s_d_date <= '${ST9}' and '${ST9}' < e_d_date
    -- and e_d_date = '3000-12-31'
  and should_repay_date between date_sub('${ST9}',30) and '${ST9}'
  and due_bill_no = '1120061514363028353120'
;



select *
from ods_new_s.loan_info
where 1 > 0
  and product_id in ('001801','001802')
  and loan_active_date = '2020-06-04'
  and overdue_date_start = '2020-06-15'
  and loan_init_term = 1
limit 50
;



invalidate metadata dw_new.dw_loan_base_stat_overdue_num_day;
select *
from dw_new.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and product_id in ('001801','001802')
  and biz_date = '2020-06-20'
  -- and remain_principal > 0
limit 50
;






set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段
set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hivevar:compute_date=2020-07-10;
set hivevar:ST9=2020-07-06;
set hivevar:db_suffix=;
set hivevar:tb_suffix=_asset;











select
  *
from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and product_id in ('001801','001802')
  and overdue_principal > 0
limit 10
;


select
  *
from dm_eagle${db_suffix}.eagle_inflow_rate_first_term_day
where 1 > 0
  and product_id in ('001801','001802')
  and overdue_principal > 0
limit 10
;


select *
from dm_eagle.eagle_credit_loan_approval_amount_sum_day
where 1 > 0
  and biz_date = '2020-06-04'
;





invalidate metadata
dm_eagle.eagle_funds
;

select
  *
from
dm_eagle.eagle_funds
-- dm.dm_watch_funds
where 1 > 0
  and biz_date = '2020-06-02'
  -- and d_date = '2020-06-02'
;



select distinct
  due_bill_no,
  loan_init_term,
  should_repay_date,
  product_id
from ods_new_s.repay_schedule
where 1 > 0
  and product_id in ('001801','001802')
  and loan_init_term = 3
  -- and loan_term = 2
  and should_repay_date <= '2020-06-30'
limit 50
;


invalidate metadata
dm_eagle.eagle_inflow_rate_first_term_day
;

select *
from dm_eagle.eagle_inflow_rate_first_term_day
where 1 > 0
  and product_id in ('001801','001802')
  and (overdue_loan_inflow_rate > 100
    or overdue_principal_inflow_rate > 100
    or remain_principal_inflow_rate > 100)
  -- and loan_init_term = 3
  -- and loan_term = 2
  -- and should_repay_date <= '2020-06-30'
limit 50
;




select
  *
from dm_eagle.eagle_loan_info
where due_bill_no = '1120060215213608230275'
order by biz_date
;



invalidate metadata
ods_new_s.loan_info
;

select *
from ods_new_s.loan_info
where due_bill_no = '1120060215213608230275'
order by s_d_date
;


select
loan_status,
loan_init_term,
curr_term,
remain_term,
paid_principal,
paid_interest,
paid_out_date,
d_date
from ods.ecas_loan_asset
where due_bill_no = '1120060215213608230275'
order by d_date
;



select distinct
  gender,
  bill_sex
from dm_eagle.assets_distribution
;




select distinct
  -- ret_msg  as resp_msg
  product_code
from ods.t_personas
where 1 > 0
  and lower(pass) = 'no'
;



select distinct
  -- ret_msg
  product_id
from
-- dm_eagle.assets_distribution
dm.dm_assets_distribution
;


select distinct ret_msg
from dm_eagle.eagle_ret_msg_day
where 1 > 0
  and ret_msg_stage = '02'
  and product_id = '001802'
  and biz_date < '2020-06-25'
;







select due_bill_no,loan_init_term,overdue_prin,d_date,(loan_init_prin - paid_principal) as remain_principal
from ods.ecas_loan_asset
where 1 > 0
  and product_code in (
    '001801'
    -- ,
    -- '001802'
  )
  and active_date = '2020-06-04'
  and overdue_days = 1
order by d_date
-- limit 10
;




select max(s_d_date)
from ods_new_s_cps.loan_info
;






invalidate metadata dw_new.dw_loan_base_stat_overdue_num_day;


select distinct loan_active_date
from dw_new.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and product_id in ('001801')
order by loan_active_date
;




select
  loan_active_date,
  overdue_days,
  sum(if(overdue_days > 0,remain_principal,0)) as cnt
from ods_new_s.loan_info
where 1 > 0
  and product_id = '001801'
  and s_d_date <= '2020-07-15'
  and e_d_date > '2020-07-15'
  and loan_active_date = '2020-06-25'
  and loan_init_term = 3
  and loan_term = 2
group by loan_active_date,overdue_days
;




invalidate metadata dm_eagle.eagle_credit_loan_approval_amount_sum_day;
select
  *
from dm_eagle.eagle_credit_loan_approval_amount_sum_day
where 1 > 0
  and biz_date = '2020-06-04'
;


select distinct
  education,bill_education
from dm_eagle.assets_distribution
;





select array('a','b','c') as a;






select
  split(
    concat_ws(',',
      if(overdue_days = 0,  '0',   null),
      if(overdue_days >= 1, '1+',  null),
      if(overdue_days > 3,  '3+',  null),
      if(overdue_days > 7,  '7+',  null),
      if(overdue_days > 14, '14+', null),
      if(overdue_days > 30, '30+', null),
      if(overdue_days > 60, '60+', null),
      if(overdue_days > 90, '90+', null),
      if(overdue_days > 120,'120+',null),
      if(overdue_days > 150,'150+',null),
      if(overdue_days > 180,'180+',null)
    ),','
  ) as tt
from (
  select 50  as overdue_days union all
  select 100 as overdue_days
) as tmp
;




select *
  -- due_bill_no,
  -- -- loan_status,
  -- biz_date
from
ods_new_s.repay_detail
-- ods_new_s_cps.repay_detail
where 1 > 0
  -- and biz_date = '2020-06-03'
  -- and loan_status = '0'
  and due_bill_no in (
    '1120060215213608230275',
    '1120060216004289090275',
    '1120060315434201160683',
    '1120060318015544273567',
    '1120060420501158464265',
    '1120060420510334902509',
    '1120060510291041444850',
    '1120060510291180650917',
    '1120060510291219831303',
    '1120060510292278025855',
    '1120060510292928006247',
    '1120060510293108891287',
    '1120060510293236264396',
    '1120060510295114945051',
    '1120060510295545340062',
    '1120060510300559326682',
    '1120060510300702357685',
    '1120060510300944421982',
    '1120060510303029749221',
    '1120060510313310319606',
    '1120060510313484801510',
    '1120060510314443303533',
    '1120060510314637234204',
    '1120060510324386931800',
    '1120060510334464331333'

    -- '1120060510334912690618',
    -- '1120060510343157147367',
    -- '1120060510450998656515',
    -- '1120060510452658560402',
    -- '1120060510455040992322',
    -- '1120060510455756944221',
    -- '1120060510462216533625',
    -- '1120060510464638220703',
    -- '1120060510470989937022',
    -- '1120060510471257615809',
    -- '1120060510471442052118',
    -- '1120060510472892852410',
    -- '1120060510474405236124',
    -- '1120060510483166645034',
    -- '1120060510483511719117',
    -- '1120060510491202120934',
    -- '1120060511110961602615',
    -- '1120060511120964383329',
    -- '1120060511131974539713',
    -- '1120060511132383103016',
    -- '1120060511140642596833',
    -- '1120060511141298693031',
    -- '1120060511173419906330',
    -- '1120060511174298184833',
    -- '1120060511184385512833',
    -- '1120060511312194364430',
    -- '1120060511315016692632',
    -- '1120060511325696044931',
    -- '1120060602304430867548',
    -- '1120060602304689935795',
    -- '1120060602322140630323',
    -- '1120060602323631368511',
    -- '1120060602324557511307',
    -- '1120060602332594205233',
    -- '1120060602343409571806',
    -- '1120060602450492475795',
    -- '1120060602480529513432',
    -- '1120060602543338694112'
  )
-- order by product_id,due_bill_no,order_id
order by due_bill_no,biz_date,order_id
;


select distinct
  txn_date
from
-- ods.ecas_repay_hst_asset
ods.ecas_repay_hst
where 1 > 0
  -- and txn_date = '2020-06-03'
  -- and due_bill_no = '1120060216004289090275'
-- and d_date = '2020-06-04'
-- order by due_bill_no,order_id
order by txn_date
;




insert overwrite table ods_new_s.repay_detail_tmp partition(biz_date,product_id)
select *
from ods_new_s.repay_detail
where 1 > 0
  and biz_date in ('2020-06-03','2020-06-04','2020-06-05','2020-06-06')
  and loan_status != '0'
-- order by product_id,biz_date,payment_id
;

insert overwrite table ods_new_s.repay_detail partition(biz_date,product_id)
select *
from ods_new_s.repay_detail_tmp
where 1 > 0
  and biz_date in ('2020-06-03','2020-06-04','2020-06-05','2020-06-06')
-- order by product_id,biz_date,payment_id
;



invalidate metadata ods_new_s.repay_detail;

select
  product_id,
  biz_date,
  sum(repay_amount) as paid_amount_new
from ods_new_s.repay_detail
where 1 > 0
  and product_id in ('001801','001802')
  and biz_date in ('2020-06-03','2020-06-04','2020-06-05','2020-06-06')
  -- and biz_date <= '2020-06-18'
group by product_id,biz_date
order by product_id,biz_date
;


select
  product_id,
  txn_date,
  sum(repay_amt) as paid_amount_ods
from ods.ecas_repay_hst_asset as tmp
join (select distinct due_bill_no,product_code as product_id from ods.ecas_loan_asset where product_code in ('001801','001802')) as loan
on tmp.due_bill_no = loan.due_bill_no
where 1 > 0
  and d_date = '2020-08-11'
  and txn_date in ('2020-06-03','2020-06-04','2020-06-05','2020-06-06')
  -- and txn_date <= '2020-06-18'
group by product_id,txn_date
order by product_id,txn_date
;



select count(1)
from ods.ecas_repay_hst_asset as tmp
where d_date = '2020-08-11'
;







invalidate metadata ods_new_s.repay_detail;
invalidate metadata ods.ecas_repay_hst_asset;
invalidate metadata ods.ecas_loan_asset;
invalidate metadata ods_new_s_cps.repay_detail;
invalidate metadata ods.ecas_repay_hst;
invalidate metadata ods.ecas_loan;

select
  coalesce(repay_detail.product_id,repay_hst.product_id,repay_detail_cps.product_id,repay_detail_cps.product_id) as product_id,
  coalesce(repay_detail.txn_date,repay_hst.txn_date,repay_detail_cps.txn_date,repay_detail_cps.txn_date)         as txn_date,
  paid_amount_new,
  paid_amount_ods,
  paid_amount_new_cps,
  paid_amount_ods_cps
from (
  select
    product_id,
    biz_date as txn_date,
    sum(repay_amount) as paid_amount_new
  from ods_new_s.repay_detail
  where 1 > 0
    -- and biz_date = '2020-06-16'
    and product_id in ('001801','001802')
  group by product_id,biz_date
) as repay_detail
full join (
  select
    product_id,
    txn_date,
    sum(repay_amt) as paid_amount_ods
  from ods.ecas_repay_hst_asset as tmp
  join (select distinct due_bill_no,product_code as product_id from ods.ecas_loan_asset where product_code in ('001801','001802')) as loan
  on tmp.due_bill_no = loan.due_bill_no
  where 1 > 0
    -- and d_date = '2020-06-16'
    and txn_date = d_date
  group by product_id,txn_date
) as repay_hst
on  repay_detail.product_id = repay_hst.product_id
and repay_detail.txn_date   = repay_hst.txn_date





select *
from dw_new.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and biz_date = '2020-06-30'
  and overdue_days > 0
limit 10
;



select *
from dm_eagle.eagle_repay_schedule
limit 10
;

invalidate metadata
ods_new_s.customer_info
;
select distinct cutomer_type
from ods_new_s.customer_info
limit 10
;



select *
from dm_eagle.eagle_repay_detail
where 1 > 0
  and due_bill_no = '1120060215213608230275'
  -- and biz_date = '2020-06-04'
limit 10
;



select
  loan_terms,
  remain_principal,
  overdue_principal,
  unposted_principal,
  biz_date,
  product_id
from dm_eagle.eagle_asset_scale_principal_day
where 1 > 0
    and product_id in ('001801','001802')
  -- and biz_date = '2020-06-04'
limit 10
;


invalidate metadata
dm_eagle.eagle_overdue_rate_month
;
select distinct dpd
from dm_eagle.eagle_overdue_rate_month
where 1 > 0
  -- and due_bill_no = '1120060215213608230275'
  -- and biz_date = '2020-06-04'
limit 10
;


invalidate metadata
dm_eagle.eagle_repayment_record_day
;
select *
from dm_eagle.eagle_repayment_record_day
where 1 > 0
  -- and due_bill_no = '1120060215213608230275'
  -- and biz_date = '2020-06-04'
limit 10
;



invalidate metadata
dm_eagle.eagle_repayment_record_day
;
select *
from dm_eagle.eagle_repayment_record_day
where 1 > 0
  -- and due_bill_no = '1120060215213608230275'
  -- and biz_date = '2020-06-04'
limit 10
;

dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day






select *
from dm_eagle.eagle_order_info
where 1 > 0
  and order_id = '000015914454181admin000082000003'
;




select *
from
-- ods.ecas_order
-- ods.ecas_order_hst
ods.ecas_order_asset
-- ods.ecas_order_hst_asset
where 1 > 0
  and d_date = '2020-08-12'
  and order_id = '000015914454181admin000082000003'
;





