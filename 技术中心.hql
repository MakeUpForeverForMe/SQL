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
-- INSERT OVERWRITE TABLE dwb.dwb_dd_log_detail PARTITION(d_date)
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
-- INSERT OVERWRITE TABLE dwb.dwb_bussiness_customer_info PARTITION(d_date)
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
-- INSERT OVERWRITE TABLE dwb.dwb_bank_card_info PARTITION(d_date)
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
-- INSERT OVERWRITE TABLE dwb.dwb_bind_card_change PARTITION(d_date)
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
-- INSERT OVERWRITE TABLE dwb.dwb_repay_hst PARTITION(p_type)
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
-- INSERT OVERWRITE TABLE dwb.dwb_loan PARTITION(d_date,p_type)
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
-- INSERT OVERWRITE TABLE dwb.dwb_order PARTITION(d_date,p_type)
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
-- LOAN_RESULT，LOAN_APPLY，BIND_BANK_CARD_CHANGE，CREDIT_APPLY，REPAY_RESULT，BIND_BANK_CARD，CREDIT_CHANGE
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






select
  org           as org,
  applicationid as apply_id,
  -- ecif_id as ecif_id,
  ecif as ecif_id,
  dwb_dd_log_detail.product_code as product_code,
  dim_product.channel_id as channel_id,
  applicationid as apply_no,
  substring(applicationid,11,8) as apply_date,
  datefmt(substring(applicationid,11,12),'yyyyMMddHHmm','yyyy-MM-dd HH:mm:ss') as apply_time,
  amount as apply_amt,
  null as contr_no,
  datefmt(substring(applicationid,11,12),'yyyyMMddHHmm','yyyy-MM-dd HH:mm:ss') as credit_time,
  amount as limit_amt,
  startdate as start_date,
  enddate as end_date,
  substring(applicationid,11,8) as process_date,
  if(creditresultstatus = 'Yes','Y','N') as apply_status,
  if(creditresultstatus = 'Yes','Y','N') as resp_msg,
  null as apply_type,
  null as resp_code,
from
-- dwb.dwb_dd_log_detail
(select *,'DIDI201908161538' as product_code from dwb.dwb_dd_log_detail) as dwb_dd_log_detail
left join (
  select product_code,channel_id from dim.dim_product
) as dim_product
on dwb_dd_log_detail.product_code = dim_product.product_code
limit 10;







desc dwb.dwb_credit_apply;

select distinct resp_msg from dwb.dwb_credit_apply;

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
desc dim.dim_scene_channel;
select * from dim.dim_scene_channel;
desc dim.dim_scene_product;
select * from dim.dim_scene_product;



























select
  as capital_id,           -- string        COMMENT '资金方编号',
  as channel_id,           -- string        COMMENT '渠道方编号',
  as project_id,           -- string        COMMENT '项目编号',
  as product_id,           -- string        COMMENT '产品编号',
  as cust_id,              -- string        COMMENT '客户编号（渠道方编号—用户编号）',
  as user_hash_no,         -- string        COMMENT '用户编号',
  as ecif_id,              -- string        COMMENT 'ecif_id',
  as apply_id,             -- string        COMMENT '授信申请编号',
  as credit_apply_time,    -- timestamp     COMMENT '授信申请时间（yyyy—MM—dd HH:mm:ss）',
  as apply_amount,         -- decimal(15,4) COMMENT '申请金额',
  as rc_assessment_time,   -- timestamp     COMMENT '风控评估时间（yyyy—MM—dd HH:mm:ss）',
  as rc_type,              -- string        COMMENT '风控类型（用信风控、二次风控）',
  as apply_status,         -- string        COMMENT '授信申请结果',
  as rc_result_validity,   -- timestamp     COMMENT '风控结果有效期（yyyy—MM—dd HH:mm:ss）',
  as resp_code,            -- string        COMMENT '授信结果码',
  as resp_msg,             -- string        COMMENT '结果描述',
  as credit_amount,        -- decimal(15,4) COMMENT '授信额度',
  as credit_interest_rate, -- decimal(15,8) COMMENT '授信利率',
  as risk_level,           -- string        COMMENT '风控等级',
  as risk_score,           -- string        COMMENT '风控评分',
  as ori_request,          -- string        COMMENT '原始请求',
  as ori_response,         -- string        COMMENT '原始应答',
  as credit_expire_date    -- timestamp     COMMENT '授信截止时间（yyyy—MM—dd HH:mm:ss）'
from

















select
  as  capital_id,        -- '资金方编号',
  as  channel_id,        -- '渠道方编号',
  as  project_id,        -- '项目编号',
  as  product_id,        -- '产品编号',
  as  cust_id,           -- '客户编号（渠道方编号—用户编号）',
  as  user_hash_no,      -- '用户编号',
  as  ecif_id,           -- 'ecif_id',
  as  outer_cust_id,     -- '外部客户号',
  as  idcard_type,       -- '证件类型（身份证等）',
  as  idcard_no,         -- '证件号码',
  as  name,              -- '客户姓名',
  as  mobie,             -- '客户电话',
  as  gender,            -- '客户性别',
  as  birthday,          -- '出生日期',
  as  age,               -- '年龄',
  as  age_range,         -- '年龄区间（0—20、21—25、26—30、31—35、36—40、41+）',
  as  marriage_status,   -- '婚姻状态',
  as  education,         -- '学历',
  as  education_range,   -- '学历区间（大专及以下、本科、硕士、博士及以上）',
  as  overflow_amt,      -- '溢缴款',
  as  id_card_address,   -- '身份证地址',
  as  id_card_area,      -- '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  as  id_card_province,  -- '身份证省级（省/直辖市/特别行政区）',
  as  id_card_city,      -- '身份证地级（城市）',
  as  id_card_county,    -- '身份证县级（区县）',
  as  id_card_township,  -- '身份证乡级（乡/镇/街）（预留）',
  as  resident_address,  -- '常住地地址',
  as  resident_area,     -- '常住地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  as  resident_province, -- '常住地省级（省/直辖市/特别行政区）',
  as  resident_city,     -- '常住地地级（城市）',
  as  resident_county,   -- '常住地县级（区县）',
  as  resident_township, -- '常住地乡级（乡/镇/街）（预留）',
  as  job_type,          -- '工作类型',
  as  job_year,          -- '工作年限',
  as  income_month,      -- '月收入',
  as  income_year,       -- '年收入',
  as  income_year_range, -- '年收入区间（10万（不含）到15万（含））', -- ？？？
  as  cutomer_type,      -- '客戶类型（个人或企业）',
  as  create_time,       -- '创建时间（yyyy—MM—dd HH:mm:ss）',
  as  update_time        -- '更新时间（yyyy—MM—dd HH:mm:ss）'
from (
with column_common as (
  select 'a' as channel_id,'b' as capital_id union all
  select 'c' as channel_id,'d' as capital_id
)
select
  column_common.capital_id  as  capital_id,        -- '资金方编号',
  ecas_customer.channel_id  as  channel_id,        -- '渠道方编号',
  'DIDI201908161538'        as  project_id,        -- '项目编号',
  'DIDI201908161538'        as  product_id,        -- '产品编号',
  as  cust_id,           -- '客户编号（渠道方编号—用户编号）',
  as  user_hash_no,      -- '用户编号',
  as  ecif_id,           -- 'ecif_id',
  as  outer_cust_id,     -- '外部客户号',
  as  idcard_type,       -- '证件类型（身份证等）',
  as  idcard_no,         -- '证件号码',
  as  name,              -- '客户姓名',
  as  mobie,             -- '客户电话',
  as  gender,            -- '客户性别',
  as  birthday,          -- '出生日期',
  as  age,               -- '年龄',
  as  age_range,         -- '年龄区间（0—20、21—25、26—30、31—35、36—40、41+）',
  as  marriage_status,   -- '婚姻状态',
  as  education,         -- '学历',
  as  education_range,   -- '学历区间（大专及以下、本科、硕士、博士及以上）',
  as  overflow_amt,      -- '溢缴款',
  as  id_card_address,   -- '身份证地址',
  as  id_card_area,      -- '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  as  id_card_province,  -- '身份证省级（省/直辖市/特别行政区）',
  as  id_card_city,      -- '身份证地级（城市）',
  as  id_card_county,    -- '身份证县级（区县）',
  as  id_card_township,  -- '身份证乡级（乡/镇/街）（预留）',
  as  resident_address,  -- '常住地地址',
  as  resident_area,     -- '常住地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  as  resident_province, -- '常住地省级（省/直辖市/特别行政区）',
  as  resident_city,     -- '常住地地级（城市）',
  as  resident_county,   -- '常住地县级（区县）',
  as  resident_township, -- '常住地乡级（乡/镇/街）（预留）',
  as  job_type,          -- '工作类型',
  as  job_year,          -- '工作年限',
  as  income_month,      -- '月收入',
  as  income_year,       -- '年收入',
  as  income_year_range, -- '年收入区间（10万（不含）到15万（含））', -- ？？？
  as  cutomer_type,      -- '客戶类型（个人或企业）',
  as  create_time,       -- '创建时间（yyyy—MM—dd HH:mm:ss）',
  as  update_time        -- '更新时间（yyyy—MM—dd HH:mm:ss）'
from ods.ecas_customer
left join column_common on ecas_customer.channel_id = column_common.channel_id
union all
select
*
from ods.ccs_customer
)


-- /**
--  * 用户信息表
--  *
--  * 数据库主键 user_hash_no
--  */
DROP TABLE IF EXISTS `ods_new_s.user_info`;
CREATE TABLE IF NOT EXISTS `ods_new_s.user_info`(
  `user_hash_no`                  string        COMMENT '用户编号（暂时取ecif_id）',
  `idcard_type`                   string        COMMENT '证件类型（身份证等）',
  `idcard_no`                     string        COMMENT '证件号码',
  `name`                          string        COMMENT '客户姓名',
  `mobie`                         string        COMMENT '客户电话',
  `gender`                        string        COMMENT '客户性别',
  `birthday`                      date          COMMENT '出生日期',
  `marriage_status`               string        COMMENT '婚姻状态',
  `education`                     string        COMMENT '学历',
  `id_card_address`               string        COMMENT '身份证地址',
  `id_card_area`                  string        COMMENT '身份证大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `id_card_province`              string        COMMENT '身份证省级（省/直辖市/特别行政区）',
  `id_card_city`                  string        COMMENT '身份证地级（城市）',
  `id_card_county`                string        COMMENT '身份证县级（区县）',
  `id_card_township`              string        COMMENT '身份证乡级（乡/镇/街）（预留）',
  `apply_address`                 string        COMMENT '申请地地址',
  `apply_area`                    string        COMMENT '申请地大区（东北地区、华北地区、西北地区、西南地区、华南地区、华东地区、华中地区、港澳台地区）',
  `apply_province`                string        COMMENT '申请地省级（省/直辖市/特别行政区）',
  `apply_city`                    string        COMMENT '申请地地级（城市）',
  `apply_county`                  string        COMMENT '申请地县级（区县）',
  `apply_township`                string        COMMENT '申请地乡级（乡/镇/街）（预留）'
) COMMENT '用户信息表'
STORED AS PARQUET;








select
    as  capital_id,       --  '资金方编号',
    as  channel_id,       --  '渠道方编号',
    as  project_id,       --  '项目编号',
    as  product_id,       --  '产品编号',
    as  cust_id,          --  '客户编号（渠道方编号—用户编号）',
    as  user_hash_no,     --  '用户编号',
    as  ecif_id,          --  'ecif_id',
    as  due_bill_no,      --  '借据编号',
    as  card_id,          --  '绑卡编号',
    as  bank_card_flag,   --  '绑卡标志（N：正常，F：非客户本人、共同借款人、配偶）',
    as  bank_card_id_no,  --  '证件号码',
    as  bank_card_name,   --  '姓名',
    as  bank_card_phone,  --  '手机号',
    as  bank_card_no,     --  '银行卡号',
    as  pay_channel,      --  '支付渠道（1：宝付，2：通联）',
    as  agreement_no,     --  '绑卡协议编号',
    as  is_valid,         --  '是否生效',
    as  is_default,       --  '是否默认卡',
    as  effective_time,   --  '生效时间（yyyy—MM—dd HH:mm:ss）',
    as  expire_time       --  '失效时间（yyyy—MM—dd HH:mm:ss）'
from ods.ecas_bind_card as bind_card
left join ods.ecas_bind_card_change as bind_card_change
on bind_card. = bind_card_change.










