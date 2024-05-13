CREATE EXTERNAL TABLE analyticsb2b_sb.ext_rkahmetov_analit_mob (
	date_x date NULL,
	"period" float8 NULL,
	inn varchar(25) NULL,
	inn_as_is varchar(25) NULL,
	is_con_inn varchar(25) NULL,
	kontr_name varchar(300) NULL,
	kontr_name_as_is varchar(300) NULL,
	region float8 NULL,
	region_txt varchar(50) NULL,
	reg_week2 varchar(50) NULL,
	tr varchar(15) NULL,
	tariff_id_prev float8 NULL,
	tariff_prev varchar(150) NULL,
	tariff_id float8 NULL,
	tariff varchar(150) NULL,
	tariff_mgr float8 NULL,
	tariff_id_as_is float8 NULL,
	tariff_as_is varchar(150) NULL,
	tp_group_id_prev float8 NULL,
	tp_group_prev varchar(50) NULL,
	tp_group_id float8 NULL,
	tp_group varchar(50) NULL,
	tp_group_mgr float8 NULL,
	tp_group_id_as_is float8 NULL,
	tp_group_as_is varchar(50) NULL,
	tp_big_prev varchar(50) NULL,
	tp_big varchar(50) NULL,
	tp_big_mgr float8 NULL,
	tp_big_as_is varchar(50) NULL,
	m_categ_id_prev float8 NULL,
	m_categ_prev varchar(100) NULL,
	m_categ_id float8 NULL,
	m_categ varchar(100) NULL,
	m_categ_mgr float8 NULL,
	m_categ_id_as_is float8 NULL,
	m_categ_as_is varchar(100) NULL,
	segment_prev varchar(50) NULL,
	"segment" varchar(50) NULL,
	segment_mgr float8 NULL,
	segment_as_is varchar(50) NULL,
	activation_marker float8 NULL,
	abonent_activation_month float8 NULL,
	arpu_abonent_categ_mob_total varchar(25) NULL,
	arpu_abonent_categ_mob_no_vas varchar(25) NULL,
	fclc_total float8 NULL,
	fclc_prochie_mob float8 NULL,
	fclc_gprs float8 NULL,
	fclc_sms float8 NULL,
	fclc_vsr float8 NULL,
	fclc_vz_mg_mn float8 NULL,
	fclc_abon_plata float8 NULL,
	fclc_prochie_vas float8 NULL,
	fclc_mob_total float8 NULL,
	fclc_mob_no_vas float8 NULL,
	fclc_total_prev float8 NULL,
	fclc_mob_total_prev float8 NULL,
	fclc_mob_no_vas_prev float8 NULL,
	pritok_mob_total varchar(50) NULL,
	pritok_mob_no_vas varchar(50) NULL,
	pritok_mob_total_all varchar(50) NULL,
	pritok_mob_no_vas_all varchar(50) NULL,
	ottok_mob_total varchar(50) NULL,
	ottok_mob_no_vas varchar(50) NULL,
	ottok_mob_total_all varchar(50) NULL,
	ottok_mob_no_vas_all varchar(50) NULL,
	pritok_mob_total_as_is varchar(50) NULL,
	pritok_mob_no_vas_as_is varchar(50) NULL,
	pritok_mob_total_all_as_is varchar(50) NULL,
	pritok_mob_no_vas_all_as_is varchar(50) NULL,
	ottok_mob_total_as_is varchar(50) NULL,
	ottok_mob_no_vas_as_is varchar(50) NULL,
	ottok_mob_total_all_as_is varchar(50) NULL,
	ottok_mob_no_vas_all_as_is varchar(50) NULL,
	arpu_abonent_categ_ottoka_mob_total varchar(25) NULL,
	arpu_abonent_categ_ottoka_mob_no_vas varchar(25) NULL,
	avg_do_ottoka_abonenta_mob_total float8 NULL,
	avg_do_ottoka_abonenta_mob_no_vas float8 NULL,
	rl_po_ottoku_abonenta_mob_total float8 NULL,
	rl_po_ottoku_abonenta_mob_no_vas float8 NULL,
	sum_status_mob_total float8 NULL,
	sum_status_mob_no_vas float8 NULL,
	categ_proc_status_mob_total varchar(25) NULL,
	categ_proc_status_mob_no_vas varchar(25) NULL,
	ottok_last_month_mob_total float8 NULL,
	ottok_last_month_mob_no_vas float8 NULL,
	mark_zak_block float8 NULL,
	month_zak_block float8 NULL,
	mark_aff_service float8 NULL,
	fclc_aff_serv float8 NULL,
	count_abonent float8 NULL,
	fclc_derzava float8 NULL,
	fclc_periodika float8 NULL,
	fclc_trafik float8 NULL,
	fclc_bus_abon float8 NULL,
	fclc_cat_number float8 NULL,
	fclc_connect float8 NULL,
	fclc_instal float8 NULL,
	fclc_other_one_time float8 NULL,
	fclc_derzava_prev float8 NULL,
	fclc_periodika_prev float8 NULL,
	fclc_trafik_prev float8 NULL,
	fclc_bus_abon_prev float8 NULL,
	fclc_cat_number_prev float8 NULL,
	fclc_connect_prev float8 NULL,
	fclc_instal_prev float8 NULL,
	fclc_other_one_time_prev float8 NULL,
	flag_clc_mob_total float8 NULL,
	flag_clc_mob_no_vas float8 NULL,
	flag_clc_mob_total_prev float8 NULL,
	flag_clc_mob_no_vas_prev float8 NULL,
	flag_clc_fclc_periodika float8 NULL,
	flag_clc_fclc_periodika_prev float8 NULL,
	flag_clc_fclc_bus_abon float8 NULL,
	flag_clc_fclc_bus_abon_prev float8 NULL,
	flag_clc_fclc_cat_number float8 NULL,
	flag_clc_fclc_cat_number_prev float8 NULL,
	fclc_mnr float8 NULL,
	fclc_mnr_prev float8 NULL,
	makr_rn int4 NULL,
	rn_1m_mob_total float8 NULL,
	rn_1m_mob_no_vas float8 NULL,
	rn_3m_mob_total float8 NULL,
	rn_3m_mob_no_vas float8 NULL
)
LOCATION ('pxf://schema_name.table_name?PROFILE=jdbc&SERVER=server_name')
FORMAT 'CUSTOM'
(formatter = 'pxfwritable_import');