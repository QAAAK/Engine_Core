from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
        "email": ["santalovdv@mts.ru"],  # santalovdv
        "email_on_failure": True,
        'start_date': "2022-06-01",
        "ssh_conn_id": "oka-analyze-en-001.krd.bd-cloud.mts.ru",
}



with DAG (
    dag_id = "Zachitnik",
    default_args=default_args,   
   schedule_interval = '0 7 * * *',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="Zachitnik",
    postgres_conn_id = "dwh_gp", # из вебинтерфейса airflow > admin > connections
    sql = """
        drop table if exists zashitnik_cvm_spr_mk;
        create temporary table zashitnik_cvm_spr_mk 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code, mk_name, segment, sub_segment
                    from crm_b2b_sb.zashitnik_cvm_spr_mk
                    where yes_no = 1		
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_mk;

        /*справочник по тарифным планам (ТП)*/
        drop table if exists zashitnik_cvm_spr_tp;
        create temporary table zashitnik_cvm_spr_tp 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code, 
                                    tariff_plan_name,
                                    first_group,
                                    second_group,
                                    third_group,
                                    fourth_group
                    from crm_b2b_sb.zashitnik_cvm_spr_tp
                    where first_group  = 1 
                       or second_group = 1
                       or third_group  = 1
                       or fourth_group = 1
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_tp;

        /*справочник по блокировкам*/
        drop table if exists zashitnik_cvm_spr_block;
        create temporary table zashitnik_cvm_spr_block 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code, block_name
                    from crm_b2b_sb.cvm_spr_block
                    where yes_no = 1		
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_block;

        /*справочник по параметрам*/
        drop table if exists cvm_parameters_all;
        create temporary table cvm_parameters_all 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select *
                    from crm_b2b_sb.cvm_parameters_all
                ) with data distributed replicated;
        analyze cvm_parameters_all;

        /*справочник по услугам*/
        drop table if exists zashitnik_cvm_spr_services;
        create temporary table zashitnik_cvm_spr_services 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code, 
                                    service_name, 
                                    not_in_first_group, 
                                    not_in_second_group, 
                                    not_in_third_group, 
                                    not_in_fourth_group, 
                                    additional_mark
                    from crm_b2b_sb.zashitnik_cvm_spr_services
                    where not_in_first_group  = 1
                       or not_in_second_group = 1
                       or not_in_third_group  = 1
                       or not_in_fourth_group = 1
                       or additional_mark 	  = 1
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_services;

        /*справочник по продуктам*/
        drop table if exists zashitnik_cvm_spr_product;
        create temporary table zashitnik_cvm_spr_product 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code
                    from crm_b2b_sb.zashitnik_cvm_spr_product
                    where yes_no = 1		
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_product;

        /*справочник по местоположению соединения*/
        drop table if exists zashitnik_cvm_spr_call_location;
        create temporary table zashitnik_cvm_spr_call_location 
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct code
                    from crm_b2b_sb.zashitnik_cvm_spr_call_location
                    where yes_no = 1		
                ) with data distributed by (code);
        analyze zashitnik_cvm_spr_call_location;


        /***получение данных***/

        /*получаем информацию по открытому ПО, которое подходит по МК и ТП*/
        drop table if exists app_con;
        create temporary table app_con
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct dapp.regid, 
                                    dapp.region, 
                                    dapp.con_n, 
                                    dapp.app_n, 
                                    dapp.phone_num,
                                    dapp.app_tar_srv,
                                    mk.mk_name,
                                    mk.segment, 
                                    mk.sub_segment,	
                                    tp.tariff_plan_name,
                                    tp.first_group							
                    from uat_v_base.dapp_app_acc_con dapp inner join zashitnik_cvm_spr_mk mk on (dapp.con_typ = mk.code)
                                                          inner join zashitnik_cvm_spr_tp tp on (dapp.app_tar_srv = tp.code)
                    where current_timestamp - interval '1 hour' between dapp.fd and dapp.td
                      and coalesce(dapp.is_closed_app,0) = 0
                ) with data distributed by (regid, app_n);
        analyze app_con;

        /*получаем информацию по блокировкам*/
        drop table if exists app_block_temp;
        create temporary table app_block_temp
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct app.*,
                                    coalesce(block.code,0) as block
                    from app_con app left join uat_v_base.dapp_sblo block on (app.regid = block.regid and app.app_n = block.app_n and current_timestamp - interval '1 hour' between block.date_from and block.date_to)
                ) with data distributed by (regid, app_n);
        analyze app_block_temp;

        /*исключаем данные с неподходящей блокировкой*/
        drop table if exists app_block;
        create temporary table app_block
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct app.*, block.block_name
                    from app_block_temp app inner join zashitnik_cvm_spr_block block on (app.block = block.code)
                ) with data distributed by (regid, app_n);
        analyze app_block;

        /*получаем информацию по голосу и трафику за 3 месяца + текущий месяц*/
        drop table if exists app_voice_internet_step1;
        create temporary table app_voice_internet_step1
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.app_tar_srv,
                            app.first_group,
                            app.block_name,
                            (coalesce(out_mts_home_tar_dur,0) + coalesce(out_mob_tar_dur,0)  + coalesce(out_bee_tar_dur,0) 
                          + coalesce(out_mts_reg_tar_dur,0)   + coalesce(out_lcl_tar_dur,0)  + coalesce(outcity_obl_tar_dur,0) 
                          + coalesce(outcity_mr_tar_dur,0)    + coalesce(outcity_rus_tar_dur,0)) as voice,
                           (coalesce(gprs_vol,0)) as internet					
                    from app_block app inner join uat_v_base.asrv_cdtl cdtl on (app.regid = cdtl.regid and app.app_n = cdtl.app_n)
                    where cdtl.table_business_month >= (date_trunc('MONTH', current_date) - interval '3 MONTH')::date						
                  ) with data distributed by (regid, app_n);
        analyze app_voice_internet_step1;

        drop table if exists app_voice_internet_step2;
        create temporary table app_voice_internet_step2
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            sum(voice)/60 voice_sum,
                            sum(internet) as internet_sum
                    from app_voice_internet_step1 app	
                    group by app.regid,
                             app.region,
                             app.con_n,
                             app.app_n,
                             app.phone_num,
                             app.app_tar_srv,
                             app.first_group,					 
                             app.mk_name,
                             app.segment, 
                             app.sub_segment,
                             app.tariff_plan_name,
                             app.block_name
                  ) with data distributed by (regid, app_n);
        analyze app_voice_internet_step2;

        drop table if exists pre_app_voice_internet;
        create temporary table pre_app_voice_internet
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum
                    from app_voice_internet_step2 app
                    where app.voice_sum >= (select parameter_value 
                                            from cvm_parameters_all 
                                            where parameter_name = 'Минимальный голосовой трафик за 3М в РФ от (минут):')
                      and app.internet_sum >= (select parameter_value 
                                               from cvm_parameters_all 
                                               where parameter_name = 'Минимальный ПД трафик за 3М от (Mb):')
                  ) with data distributed by (regid, app_n);
        analyze pre_app_voice_internet;

        drop table if exists app_voice_internet;
        create temporary table app_voice_internet
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,				
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum
                    from pre_app_voice_internet app
                    where app.voice_sum >= (select parameter_value 
                                            from cvm_parameters_all 
                                            where parameter_name = 'Весь голосовой трафик за 3М в РФ от (минут):')
                       or app.internet_sum >= (select parameter_value 
                                               from cvm_parameters_all
                                               where parameter_name = 'Весь ПД трафик за 3М от (Mb):')
                  ) with data distributed by (regid, app_n);
        analyze app_voice_internet;

        /*получаем информацию по начислениям*/
        drop table if exists app_fin_step1;
        create temporary table app_fin_step1
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.app_n,
                            coalesce(fin.bsi_amount,0) + coalesce(fin.bsi_taxes,0) as amount_3m_t
                    from app_voice_internet app inner join uat_v_base.dfin_asclc fin on (app.regid = fin.regid and app.app_n = fin.app_n) 
                                                inner join zashitnik_cvm_spr_call_location cl  on (fin.call_location=cl.code)
                                                inner join zashitnik_cvm_spr_product pr 		 on (fin.product=pr.code)
                    where fin.table_business_month between (date_trunc('MONTH', current_date) - interval '3 MONTH')::date 
                                                       and (date_trunc('MONTH', current_date) - interval '1 MONTH')::date							
                  ) with data distributed by (regid, app_n);
        analyze app_fin_step1;

        drop table if exists app_fin;
        create temporary table app_fin
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.app_n,
                            sum(amount_3m_t)/3 as amount_3m
                    from app_fin_step1 app 
                    group by app.regid,
                             app.app_n
                  ) with data distributed by (regid, app_n);
        analyze app_fin;

        drop table if exists app_amount;
        create temporary table app_amount
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum,
                            coalesce(af.amount_3m,0) as amount_3m
                    from pre_app_voice_internet app left join app_fin af on (app.regid = af.regid and app.app_n = af.app_n)
                  ) with data distributed by (regid, app_n);
        analyze app_amount;



        /**/
        drop table if exists app_get_customer_n;
        create temporary table app_get_customer_n
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum,
                            app.amount_3m,
                            cl.customer_n,
                            cl.mrgn_id
                    from app_amount app inner join uat_v_base.da_app_acc_con_cust_lnk cl on (app.regid = cl.regid and app.app_n = cl.app_n)
                    where current_timestamp - interval '1 hour' between cl.effective_from and cl.effective_to
                  ) with data distributed by (regid, app_n);
        analyze app_get_customer_n;

        /*идём за ИНН*/

        create or replace view crm_b2b_sb.zashitnik_cvm_organization_all as 
        select mrgn_id, customer_n, inn from uat_v_dq_mr00.organization_mr00
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr01.organization_mr01
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr03.organization_mr03
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr04.organization_mr04
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr06.organization_mr06
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr07.organization_mr07
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr08.organization_mr08
        union all
        select mrgn_id, customer_n, inn from uat_v_dq_mr09.organization_mr09;

        drop table if exists pre_app_get_inn;
        create temporary table pre_app_get_inn
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            app.con_n,
                            app.app_n,
                            app.phone_num,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum,
                            app.amount_3m,
                            app.customer_n,
                            inn.inn
                    from app_get_customer_n app left join crm_b2b_sb.zashitnik_cvm_organization_all inn on (app.mrgn_id = inn.mrgn_id and app.customer_n = inn.customer_n)
                  ) with data distributed by (regid, app_n);
        analyze pre_app_get_inn;

        drop table if exists count_inn;
        create temporary table count_inn
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select app.regid, 
                           app.app_n, 
                           count(app.app_n) over (partition by app.inn) as count_app_inn
                    from pre_app_get_inn app
                    where app.inn is not null
                  ) with data distributed by (regid, app_n);
        analyze count_inn;

        drop table if exists app_get_inn;
        create temporary table app_get_inn
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  app.regid,
                            app.region,
                            spt_reg."label" as region_name,
                            app.con_n,
                            app.app_n,
                            '7'||app.phone_num as msisdn,
                            app.app_tar_srv,
                            app.first_group,					
                            app.mk_name,
                            app.segment, 
                            app.sub_segment,
                            app.tariff_plan_name,
                            app.block_name,
                            app.voice_sum,
                            app.internet_sum,
                            app.amount_3m,
                            app.customer_n,
                            app.inn,
                            count(app.app_n) over (partition by app.con_n) as count_app_con,
                            inn.count_app_inn
                    from pre_app_get_inn app left join count_inn inn on (app.regid = inn.regid and app.app_n = inn.app_n)
                                             inner join uat_v_base.sas_fmt_fsrgc spt_reg on (app.region = spt_reg."end")
                  ) with data distributed by (regid, app_n);
        analyze app_get_inn;

        /*Кол-во по <=3* (вместо app_get_inn (count_app_con))*/

        drop table if exists app_get_3po;
        create temporary table app_get_3po
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select  * from app_get_inn
                    Where count_app_con <= 3
                  ) with data distributed by (regid, app_n);
        analyze app_get_3po;



        /*получаем информацию по услугам*/
        drop table if exists pre_app_srvp;
        create temporary table pre_app_srvp
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct app.*,
                                    coalesce(spr_srv.not_in_first_group,0)  as pre_not_in_first_group, 							
                                    coalesce(spr_srv.additional_mark,0) 	as pre_additional_mark
                    from app_get_3po app inner join uat_v_base.dsrv_srvp srvp on (app.regid = srvp.regid and app.app_n = srvp.app_n and current_timestamp - interval '1 hour' between srvp.date_from and srvp.date_to)
                                         left  join zashitnik_cvm_spr_services spr_srv  on (srvp.service = spr_srv.code)
                ) with data distributed by (regid, app_n);
        analyze pre_app_srvp;

        drop table if exists app_srvp;
        create temporary table app_srvp
                with (appendonly=true, compresstype=zstd, compresslevel=1, orientation = column)
                on commit preserve rows
                as (select distinct 	
                            app.msisdn,
                            app.count_app_con as num_param_1,
                            app.count_app_inn as num_param_2,					
                            app.con_n as num_param_4,
                            app.voice_sum as num_param_5,
                            app.internet_sum as num_param_6,
                            app.amount_3m as num_param_7,
                            app.inn as char_param_1,
                            app.mk_name as char_param_2,
                            app.segment as char_param_3, 
                            app.sub_segment as char_param_4,
                            app.tariff_plan_name as char_param_5,
                            app.region_name as char_param_6,
                            app.block_name as char_param_7,
                            app.first_group,										
                            (max(app.pre_not_in_first_group)) as not_in_first_group, 					
                            (max(app.pre_additional_mark)) as num_param_3
                    from pre_app_srvp app
                    group by 	app.msisdn,
                                app.count_app_con,
                                app.count_app_inn,
                                app.con_n,
                                app.voice_sum,
                                app.internet_sum,
                                app.amount_3m,
                                app.inn,
                                app.mk_name,
                                app.segment, 
                                app.sub_segment,
                                app.tariff_plan_name,
                                app.region_name,
                                app.block_name,
                                app.first_group							
                ) with data distributed by (msisdn);
        analyze app_srvp;



        /*чистим таблицу по первой группе и загружаем в неё данные*/
        truncate table crm_b2b_sb.b2b_zashitnik_test;
        insert into crm_b2b_sb.b2b_zashitnik (msisdn, num_param_1, num_param_2, num_param_3, num_param_4, num_param_5, num_param_6, num_param_7, char_param_1, char_param_2, char_param_3,  char_param_4, char_param_5, char_param_6, char_param_7)
            select  app.msisdn,
                    app.num_param_1,
                    app.num_param_2,
                    app.num_param_3,
                    app.num_param_4,
                    app.num_param_5,
                    app.num_param_6,
                    app.num_param_7,
                    app.char_param_1,
                    app.char_param_2,
                    app.char_param_3, 
                    app.char_param_4,
                    app.char_param_5,
                    app.char_param_6,
                    app.char_param_7
                from app_srvp app
                where app.first_group = 1
                  and app.not_in_first_group = 0;
        commit;
        analyze crm_b2b_sb.b2b_zashitnik_test;
        """
    )
    
    t1



















