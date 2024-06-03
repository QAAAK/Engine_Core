CREATE OR REPLACE VIEW fgao_sb.v_wom 
AS WITH t6 AS (
         SELECT to_char('now'::text::date::timestamp with time zone, 'YYYYMM'::text) AS current_month,
            to_char('now'::text::date - '1 mon'::interval, 'YYYYMM'::text) AS previous_month
        ), t3 AS (
         SELECT DISTINCT table_analit_mob.segment,
            table_analit_mob.region,
            table_analit_mob.tariff_id_as_is / 1000::double precision AS tp,
            sum(table_analit_mob.fclc_mob_no_vas) AS fclc,
            sum(table_analit_mob.count_abonent) AS app_n,
            sum(table_analit_mob.fclc_mob_no_vas) / sum(table_analit_mob.count_abonent) AS arpu
           FROM analyticsb2b_sb.table_analit_mob
          WHERE table_analit_mob.period = to_char('now'::text::date - '1 year'::interval, 'YYYYMM'::text)::double precision AND table_analit_mob.pritok_mob_no_vas::text <> 'Пусто'::text
          GROUP BY table_analit_mob.segment, table_analit_mob.region, table_analit_mob.tariff_id_as_is
        ), t4 AS (
         SELECT DISTINCT table_analit_mob.segment,
            table_analit_mob.region,
            table_analit_mob.tariff_id_as_is / 1000::double precision AS tp,
            sum(table_analit_mob.fclc_mob_no_vas) AS fclc,
            sum(table_analit_mob.count_abonent) AS app_n,
            sum(table_analit_mob.fclc_mob_no_vas) / sum(table_analit_mob.count_abonent) AS arpu
           FROM analyticsb2b_sb.table_analit_mob
          WHERE table_analit_mob.period = to_char('now'::text::date - '1 year'::interval - '1 month'::interval, 'YYYYMM'::text)::double precision AND table_analit_mob.pritok_mob_no_vas::text <> 'Пусто'::text
          GROUP BY table_analit_mob.segment, table_analit_mob.region, table_analit_mob.tariff_id_as_is
        ), t2 AS (
         SELECT DISTINCT a.segment,
            a.region,
            a.tp,
            sum(a.fclc_trans_part) AS fclc,
            count(DISTINCT a.app_n) AS app_n,
            sum(a.fclc_trans_part) / count(DISTINCT a.app_n)::double precision AS arpu
           FROM analyticsb2b_sb.operotchet_final a
             LEFT JOIN analyticsb2b_sb.sprav_sheta c ON c."group_id" = a."group_id"::double precision AND concat(c.sub_acc_trans, '00') = a.sub_acc_trans::text
             LEFT JOIN analyticsb2b_sb.tab_sprav_tp_big b ON a.tp_group_txt::text = b.tp_group::text
          WHERE c.product_pl_new::text = 'Моб'::text AND a.fclc_trans_part > 0::double precision AND b.tp_group_big::text <> 'M2M/IOT'::text AND a.period::text = to_char('now'::text::date - '1 mon'::interval, 'YYYYMM'::text) AND c.product_pl_det2::text <> 'Прочие VAS'::text
          GROUP BY a.region, a.segment, a.tp
        ), t1 AS (
         SELECT DISTINCT a.segment,
            a.region,
            a.tp,
            a.inn,
            sum(a.fclc_trans_part) AS fclc,
            count(DISTINCT a.app_n) AS app_n,
            sum(a.fclc_trans_part) / count(DISTINCT a.app_n)::double precision AS arpu,
            a.tariff_plan_txt
           FROM analyticsb2b_sb.operotchet_final a
             LEFT JOIN analyticsb2b_sb.sprav_sheta c ON c."group_id" = a."group_id"::double precision AND concat(c.sub_acc_trans, '00') = a.sub_acc_trans::text
             LEFT JOIN analyticsb2b_sb.tab_sprav_tp_big b ON a.tp_group_txt::text = b.tp_group::text
          WHERE c.product_pl_new::text = 'Моб'::text AND a.fclc_trans_part > 0::double precision AND b.tp_group_big::text <> 'M2M/IOT'::text AND a.period::text = to_char('now'::text::date::timestamp with time zone, 'YYYYMM'::text) AND c.product_pl_det2::text <> 'Прочие VAS'::text AND a.activation_date >= date_trunc('month', current_timestamp) AND a.activation_date <= date_trunc('month', current_timestamp) + interval '1 month -1 day'
          GROUP BY a.region, a.inn, a.segment, a.tp, a.tariff_plan_txt
        ), t5 AS (
         SELECT DISTINCT a.segment,
            count(DISTINCT a.app_n) AS app_n
           FROM analyticsb2b_sb.operotchet_final a
             LEFT JOIN analyticsb2b_sb.sprav_sheta c ON c."group_id" = a."group_id"::double precision AND concat(c.sub_acc_trans, '00') = a.sub_acc_trans::text
             LEFT JOIN analyticsb2b_sb.tab_sprav_tp_big b ON a.tp_group_txt::text = b.tp_group::text
          WHERE c.product_pl_new::text = 'Моб'::text AND a.fclc_trans_part > 0::double precision AND b.tp_group_big::text <> 'M2M/IOT'::text AND a.period::text = to_char('now'::text::date::timestamp with time zone, 'YYYYMM'::text) AND c.product_pl_det2::text <> 'Прочие VAS'::text AND a.activation_date >= date_trunc('month', current_timestamp) AND a.activation_date <= date_trunc('month', current_timestamp) + interval '1 month -1 day'
          GROUP BY a.segment
        )
 SELECT t1.segment,
    t1.region AS "Регион",
    t1.tp,
    t1.app_n,
    t1.inn,
    t1.tariff_plan_txt,
    t1.fclc AS "Начисления, руб",
    t1.arpu,
    t5.app_n AS "Кол-во ПО, шт",
    t1.app_n / t5.app_n AS "Доля от базы по сегменту",
    (t1.arpu / (t2.arpu * (t3.arpu / t4.arpu)) - 1::double precision) * 100::double precision AS "Разница ARPU от прогнозного, %"
   FROM t1
     LEFT JOIN t2 ON t1.segment::text = t2.segment::text AND t1.region = t2.region AND t1.tp = t2.tp
     LEFT JOIN t3 ON t1.segment::text = t3.segment::text AND t1.region::double precision = t3.region AND t1.tp::double precision = t3.tp
     LEFT JOIN t4 ON t1.segment::text = t4.segment::text AND t1.region::double precision = t4.region AND t1.tp::double precision = t4.tp
     LEFT JOIN t5 ON t1.segment::text = t5.segment::text
  WHERE t1.arpu < (t2.arpu * (t3.arpu / t4.arpu))
  GROUP BY t1.app_n, t1.segment, t1.region, t1.tp, t1.inn, t1.tariff_plan_txt, t1.fclc, t1.arpu, t5.app_n, t1.app_n / t5.app_n, (t1.arpu / (t2.arpu * (t3.arpu / t4.arpu)) - 1::double precision) * 100::double precision
  ORDER BY t1.app_n DESC;

-- Permissions
