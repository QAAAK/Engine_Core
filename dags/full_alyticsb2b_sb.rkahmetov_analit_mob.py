from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'santalovdv', # если не отработает, то 'owner': 'admin'
    'start_date': days_ago(0),
    'depends_on_past': False
}

with DAG (
    dag_id = "FULL_LOAD_EXTERNAL_TO_TARGET_ALYTICSB2B_SB.RKAHMETOV_ANALIT_MOB",
    default_args=default_args,   
   schedule_interval = '0 9 * * 1',
    catchup = False) as dag:

    t1 = PostgresOperator( 
    task_id ="LOAD",
    postgres_conn_id = "connection_name", # из вебинтерфейса airflow > admin > connections
    sql = """
            INSERT INTO analyticsb2b_sb.rkahmetov_analit_mob (date_x,period,inn,inn_as_is,is_con_inn,kontr_name,kontr_name_as_is,region,
            region_txt,reg_week2,tr,tariff_id_prev,tariff_prev,tariff_id,tariff,tariff_mgr,tariff_id_as_is,tariff_as_is,
            tp_group_id_prev,tp_group_prev,tp_group_id,tp_group,tp_group_mgr,tp_group_id_as_is,tp_group_as_is,tp_big_prev,
            tp_big,tp_big_mgr,tp_big_as_is,m_categ_id_prev,m_categ_prev,m_categ_id,m_categ,m_categ_mgr,m_categ_id_as_is,
            m_categ_as_is,segment_prev,segment,segment_mgr,segment_as_is,activation_marker,abonent_activation_month,arpu_abonent_categ_mob_total,
            arpu_abonent_categ_mob_no_vas,fclc_total,fclc_prochie_mob,fclc_gprs,fclc_sms,fclc_vsr,fclc_vz_mg_mn,fclc_abon_plata,fclc_prochie_vas,
            fclc_mob_total,fclc_mob_no_vas,fclc_total_prev,fclc_mob_total_prev,fclc_mob_no_vas_prev,pritok_mob_total,pritok_mob_no_vas,\
            pritok_mob_total_all,pritok_mob_no_vas_all,ottok_mob_total,ottok_mob_no_vas,ottok_mob_total_all,ottok_mob_no_vas_all,pritok_mob_total_as_is,
            pritok_mob_no_vas_as_is,pritok_mob_total_all_as_is,pritok_mob_no_vas_all_as_is,ottok_mob_total_as_is,ottok_mob_no_vas_as_is,ottok_mob_total_all_as_is,
            ottok_mob_no_vas_all_as_is,arpu_abonent_categ_ottoka_mob_total,arpu_abonent_categ_ottoka_mob_no_vas,avg_do_ottoka_abonenta_mob_total,
            avg_do_ottoka_abonenta_mob_no_vas,rl_po_ottoku_abonenta_mob_total,rl_po_ottoku_abonenta_mob_no_vas,sum_status_mob_total,
            sum_status_mob_no_vas,categ_proc_status_mob_total,categ_proc_status_mob_no_vas,ottok_last_month_mob_total,ottok_last_month_mob_no_vas,
            mark_zak_block,month_zak_block,mark_aff_service,fclc_aff_serv,count_abonent,fclc_derzava,fclc_periodika,fclc_trafik,fclc_bus_abon,
            fclc_cat_number,fclc_connect,fclc_instal,fclc_other_one_time,fclc_derzava_prev,fclc_periodika_prev,fclc_trafik_prev,fclc_bus_abon_prev,
            fclc_cat_number_prev,fclc_connect_prev,fclc_instal_prev,fclc_other_one_time_prev,flag_clc_mob_total,flag_clc_mob_no_vas,
            flag_clc_mob_total_prev,flag_clc_mob_no_vas_prev,flag_clc_fclc_periodika,flag_clc_fclc_periodika_prev,flag_clc_fclc_bus_abon,
            flag_clc_fclc_bus_abon_prev,flag_clc_fclc_cat_number,flag_clc_fclc_cat_number_prev,fclc_mnr,fclc_mnr_prev,makr_rn,rn_1m_mob_total,
            rn_1m_mob_no_vas,rn_3m_mob_total,rn_3m_mob_no_vas) 
            (SELECT date_x,period,inn,inn_as_is,is_con_inn,kontr_name,kontr_name_as_is,region,
            region_txt,reg_week2,tr,tariff_id_prev,tariff_prev,tariff_id,tariff,tariff_mgr,tariff_id_as_is,tariff_as_is,
            tp_group_id_prev,tp_group_prev,tp_group_id,tp_group,tp_group_mgr,tp_group_id_as_is,tp_group_as_is,tp_big_prev,
            tp_big,tp_big_mgr,tp_big_as_is,m_categ_id_prev,m_categ_prev,m_categ_id,m_categ,m_categ_mgr,m_categ_id_as_is,
            m_categ_as_is,segment_prev,segment,segment_mgr,segment_as_is,activation_marker,abonent_activation_month,arpu_abonent_categ_mob_total,
            arpu_abonent_categ_mob_no_vas,fclc_total,fclc_prochie_mob,fclc_gprs,fclc_sms,fclc_vsr,fclc_vz_mg_mn,fclc_abon_plata,fclc_prochie_vas,
            fclc_mob_total,fclc_mob_no_vas,fclc_total_prev,fclc_mob_total_prev,fclc_mob_no_vas_prev,pritok_mob_total,pritok_mob_no_vas,\
            pritok_mob_total_all,pritok_mob_no_vas_all,ottok_mob_total,ottok_mob_no_vas,ottok_mob_total_all,ottok_mob_no_vas_all,pritok_mob_total_as_is,
            pritok_mob_no_vas_as_is,pritok_mob_total_all_as_is,pritok_mob_no_vas_all_as_is,ottok_mob_total_as_is,ottok_mob_no_vas_as_is,ottok_mob_total_all_as_is,
            ottok_mob_no_vas_all_as_is,arpu_abonent_categ_ottoka_mob_total,arpu_abonent_categ_ottoka_mob_no_vas,avg_do_ottoka_abonenta_mob_total,
            avg_do_ottoka_abonenta_mob_no_vas,rl_po_ottoku_abonenta_mob_total,rl_po_ottoku_abonenta_mob_no_vas,sum_status_mob_total,
            sum_status_mob_no_vas,categ_proc_status_mob_total,categ_proc_status_mob_no_vas,ottok_last_month_mob_total,ottok_last_month_mob_no_vas,
            mark_zak_block,month_zak_block,mark_aff_service,fclc_aff_serv,count_abonent,fclc_derzava,fclc_periodika,fclc_trafik,fclc_bus_abon,
            fclc_cat_number,fclc_connect,fclc_instal,fclc_other_one_time,fclc_derzava_prev,fclc_periodika_prev,fclc_trafik_prev,fclc_bus_abon_prev,
            fclc_cat_number_prev,fclc_connect_prev,fclc_instal_prev,fclc_other_one_time_prev,flag_clc_mob_total,flag_clc_mob_no_vas,
            flag_clc_mob_total_prev,flag_clc_mob_no_vas_prev,flag_clc_fclc_periodika,flag_clc_fclc_periodika_prev,flag_clc_fclc_bus_abon,
            flag_clc_fclc_bus_abon_prev,flag_clc_fclc_cat_number,flag_clc_fclc_cat_number_prev,fclc_mnr,fclc_mnr_prev,makr_rn,rn_1m_mob_total,
            rn_1m_mob_no_vas,rn_3m_mob_total,rn_3m_mob_no_vas from analyticsb2b_sb.ext_rkahmetov_analit_mob)
            ;
        """
    )

t1
