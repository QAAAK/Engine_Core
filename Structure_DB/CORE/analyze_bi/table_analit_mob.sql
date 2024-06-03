-- analyze_bi.rkahmetov_analit_mob definition

-- Drop table

-- DROP TABLE analyze_bi.rkahmetov_analit_mob;

CREATE TABLE analyze_bi.rkahmetov_analit_mob (
	date_x date NULL,
	"period" text NULL,
	inn text NULL,
	inn_as_is text NULL,
	is_con_inn text NULL,
	kontr_name text NULL,
	kontr_name_as_is text NULL,
	region int2 NULL,
	region_txt text NULL,
	reg_week2 text NULL,
	tr text NULL,
	tariff_id_prev text NULL,
	tariff_prev text NULL,
	tariff_id text NULL,
	tariff text NULL,
	tariff_mgr int4 NULL,
	tariff_id_as_is text NULL,
	tariff_as_is text NULL,
	tp_group_id_prev text NULL,
	tp_group_prev text NULL,
	tp_group_id text NULL,
	tp_group text NULL,
	tp_group_mgr int4 NULL,
	tp_group_id_as_is text NULL,
	tp_group_as_is text NULL,
	tp_big_prev text NULL,
	tp_big text NULL,
	tp_big_mgr int4 NULL,
	tp_big_as_is text NULL,
	m_categ_id_prev text NULL,
	m_categ_prev text NULL,
	m_categ_id text NULL,
	m_categ text NULL,
	m_categ_mgr int4 NULL,
	m_categ_id_as_is text NULL,
	m_categ_as_is text NULL,
	segment_prev text NULL,
	"segment" text NULL,
	segment_mgr int4 NULL,
	segment_as_is text NULL,
	activation_marker int4 NULL,
	abonent_activation_month text NULL,
	arpu_abonent_categ_mob_total text NULL,
	arpu_abonent_categ_mob_no_vas text NULL,
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
	pritok_mob_total text NULL,
	pritok_mob_no_vas text NULL,
	pritok_mob_total_all text NULL,
	pritok_mob_no_vas_all text NULL,
	ottok_mob_total text NULL,
	ottok_mob_no_vas text NULL,
	ottok_mob_total_all text NULL,
	ottok_mob_no_vas_all text NULL,
	pritok_mob_total_as_is text NULL,
	pritok_mob_no_vas_as_is text NULL,
	pritok_mob_total_all_as_is text NULL,
	pritok_mob_no_vas_all_as_is text NULL,
	ottok_mob_total_as_is text NULL,
	ottok_mob_no_vas_as_is text NULL,
	ottok_mob_total_all_as_is text NULL,
	ottok_mob_no_vas_all_as_is text NULL,
	arpu_abonent_categ_ottoka_mob_total text NULL,
	arpu_abonent_categ_ottoka_mob_no_vas text NULL,
	avg_do_ottoka_abonenta_mob_total float8 NULL,
	avg_do_ottoka_abonenta_mob_no_vas float8 NULL,
	rl_po_ottoku_abonenta_mob_total float8 NULL,
	rl_po_ottoku_abonenta_mob_no_vas float8 NULL,
	sum_status_mob_total float8 NULL,
	sum_status_mob_no_vas float8 NULL,
	categ_proc_status_mob_total text NULL,
	categ_proc_status_mob_no_vas text NULL,
	ottok_last_month_mob_total text NULL,
	ottok_last_month_mob_no_vas text NULL,
	mark_zak_block int4 NULL,
	month_zak_block text NULL,
	mark_aff_service int4 NULL,
	fclc_aff_serv float8 NULL,
	count_abonent int8 NULL,
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
	flag_clc_mob_total int4 NULL,
	flag_clc_mob_no_vas int4 NULL,
	flag_clc_mob_total_prev int4 NULL,
	flag_clc_mob_no_vas_prev int4 NULL,
	flag_clc_fclc_periodika int4 NULL,
	flag_clc_fclc_periodika_prev int4 NULL,
	flag_clc_fclc_bus_abon int4 NULL,
	flag_clc_fclc_bus_abon_prev int4 NULL,
	flag_clc_fclc_cat_number int4 NULL,
	flag_clc_fclc_cat_number_prev int4 NULL,
	fclc_mnr float8 NULL,
	fclc_mnr_prev float8 NULL
)
WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY ("period")
PARTITION BY RANGE(date_x) 
          (
          START ('2021-01-01'::date) END ('2025-01-01'::date) EVERY ('1 mon'::interval) WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1') 
                    COLUMN date_x ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN period ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN inn ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN inn_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN is_con_inn ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN kontr_name ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN kontr_name_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN region ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN region_txt ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN reg_week2 ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_id_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_id ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_mgr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_id_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tariff_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_id_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_id ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_mgr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_id_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_group_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_big_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_big ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_big_mgr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN tp_big_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_id_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_id ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_mgr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_id_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN m_categ_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN segment_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN segment ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN segment_mgr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN segment_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN activation_marker ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN abonent_activation_month ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN arpu_abonent_categ_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN arpu_abonent_categ_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_prochie_mob ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_gprs ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_sms ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_vsr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_vz_mg_mn ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_abon_plata ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_prochie_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_total_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mob_total_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mob_no_vas_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_total_all ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_no_vas_all ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_total_all ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_no_vas_all ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_total_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_no_vas_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_total_all_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN pritok_mob_no_vas_all_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_total_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_no_vas_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_total_all_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_mob_no_vas_all_as_is ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN arpu_abonent_categ_ottoka_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN arpu_abonent_categ_ottoka_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN avg_do_ottoka_abonenta_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN avg_do_ottoka_abonenta_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN rl_po_ottoku_abonenta_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN rl_po_ottoku_abonenta_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN sum_status_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN sum_status_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN categ_proc_status_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN categ_proc_status_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_last_month_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN ottok_last_month_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN mark_zak_block ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN month_zak_block ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN mark_aff_service ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_aff_serv ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN count_abonent ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_derzava ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_periodika ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_trafik ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_bus_abon ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_cat_number ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_connect ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_instal ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_other_one_time ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_derzava_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_periodika_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_trafik_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_bus_abon_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_cat_number_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_connect_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_instal_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_other_one_time_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_mob_total ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_mob_no_vas ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_mob_total_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_mob_no_vas_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_periodika ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_periodika_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_bus_abon ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_bus_abon_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_cat_number ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN flag_clc_fclc_cat_number_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mnr ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768) 
                    COLUMN fclc_mnr_prev ENCODING (compresstype=zstd, compresslevel=1, blocksize=32768)
          );

-- Permissions

ALTER TABLE analyze_bi.rkahmetov_analit_mob OWNER TO analyze_bi_owner;
GRANT ALL ON TABLE analyze_bi.rkahmetov_analit_mob TO analyze_bi_owner;
GRANT SELECT ON TABLE analyze_bi.rkahmetov_analit_mob TO analyze_bi_ro;