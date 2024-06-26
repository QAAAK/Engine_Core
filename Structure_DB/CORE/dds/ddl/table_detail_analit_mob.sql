-- Hive.analyticsb2b_sb.vas_iot_otchet definition

-- Drop table

-- DROP TABLE Hive.analyticsb2b_sb.vas_iot_otchet;






CREATE table  dds.detail_analyt_mob (
	  id bigint default nextval('dds.sequence_detail_analyt_mob'::regclass) not null,
      inn text,
      app_n DECIMAL(20,0),
      acc_n text,
      con_n text,
      region SMALLINT,
      activation_date TIMESTAMP,
      phone_num BIGINT,
      kontr_name text,
      app_class DECIMAL(9,3),
      app_class1 text,
      con_typ text,
      con_typ1 text,
      tariff_plan text,
      tariff_plan1 text,
      tp_group text,
      tp_group1 text,
      service_id DECIMAL(10,3),
      service_id1 text,
      service_type INT,
      service_type1 text,
      tariff_zone_id DECIMAL(10,3),
      tariff_zone_id1 text,
      network_operator_id DECIMAL(10,3),
      network_operator_id1 text,
      place_of_call INT,
      place_of_call1 text,
      group_id text,
      group_id1 text,
      trans_part double precision,
      bsi_amount_trans_part double precision,
      bsi_discount_trans_part double precision,
      fclc_trans_part double precision,
      product_oebs double precision,
      product_oebs1 text,
      mcat_business double precision,
      sub_acc_trans text,
      is_sub_acc_trans_in_list INT,
      subproduct text,
      table_business_month text,
      rn_coefficient text,
      first_date TIMESTAMP,
      final_product double precision,
      final_product1 text,
      new_po INT,
      rn_1m_bsi_amount_trans_part double precision,
      rn_1m_bsi_discount_trans_part double precision,
      rn_3m_bsi_amount_trans_part double precision,
      rn_3m_bsi_discount_trans_part double precision,
      is_b2b INT,
      final_product_name text,
      regid INT,
      vgr INT,
      fio_otvet_manager text,
      fr_otvet_manage text,
      pm text,
      fr_pm text,
      mnk text,
      name_of_holding text,
      number_of_holding DECIMAL(10,0),
      rn_1m double precision,
      rn_3m double precision,
      podsegment text,
      ddtm_insert date default now() not null
) WITH (
	appendonly=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (id, region)

partition  by list (region) (
partition detail_analyt_mob_prt_1    values(1),
partition detail_analyt_mob_prt_2    values(2),
partition detail_analyt_mob_prt_3    values(3),
partition detail_analyt_mob_prt_4    values(4),
partition detail_analyt_mob_prt_5    values(5),
partition detail_analyt_mob_prt_6    values(6),
partition detail_analyt_mob_prt_7    values(7),
partition detail_analyt_mob_prt_8    values(8),
partition detail_analyt_mob_prt_9    values(9),
partition detail_analyt_mob_prt_10    values(10),
partition detail_analyt_mob_prt_11    values(11),
partition detail_analyt_mob_prt_12    values(12),
partition detail_analyt_mob_prt_13    values(13),
partition detail_analyt_mob_prt_14    values(14),
partition detail_analyt_mob_prt_15    values(15),
partition detail_analyt_mob_prt_16    values(16),
partition detail_analyt_mob_prt_17    values(17),
partition detail_analyt_mob_prt_18    values(18),
partition detail_analyt_mob_prt_19    values(19),
partition detail_analyt_mob_prt_20    values(20),
partition detail_analyt_mob_prt_21    values(21),
partition detail_analyt_mob_prt_22    values(22),
partition detail_analyt_mob_prt_23    values(23),
partition detail_analyt_mob_prt_24    values(24),
partition detail_analyt_mob_prt_25    values(25),
partition detail_analyt_mob_prt_26    values(26),
partition detail_analyt_mob_prt_27    values(27),
partition detail_analyt_mob_prt_28    values(28),
partition detail_analyt_mob_prt_29    values(29),
partition detail_analyt_mob_prt_30    values(30),
partition detail_analyt_mob_prt_31    values(31),
partition detail_analyt_mob_prt_32    values(32),
partition detail_analyt_mob_prt_33    values(33),
partition detail_analyt_mob_prt_34    values(34),
partition detail_analyt_mob_prt_35    values(35),
partition detail_analyt_mob_prt_36    values(36),
partition detail_analyt_mob_prt_37    values(37),
partition detail_analyt_mob_prt_38    values(38),
partition detail_analyt_mob_prt_39    values(39),
partition detail_analyt_mob_prt_40    values(40),
partition detail_analyt_mob_prt_41    values(41),
partition detail_analyt_mob_prt_42    values(42),
partition detail_analyt_mob_prt_43    values(43),
partition detail_analyt_mob_prt_44    values(44),
partition detail_analyt_mob_prt_45    values(45),
partition detail_analyt_mob_prt_46    values(46),
partition detail_analyt_mob_prt_47    values(47),
partition detail_analyt_mob_prt_48    values(48),
partition detail_analyt_mob_prt_49    values(49),
partition detail_analyt_mob_prt_50    values(50),
partition detail_analyt_mob_prt_51    values(51),
partition detail_analyt_mob_prt_52    values(52),
partition detail_analyt_mob_prt_53    values(53),
partition detail_analyt_mob_prt_54    values(54),
partition detail_analyt_mob_prt_55    values(55),
partition detail_analyt_mob_prt_56    values(56),
partition detail_analyt_mob_prt_57    values(57),
partition detail_analyt_mob_prt_58    values(58),
partition detail_analyt_mob_prt_59    values(59),
partition detail_analyt_mob_prt_60    values(60),
partition detail_analyt_mob_prt_61    values(61),
partition detail_analyt_mob_prt_62    values(62),
partition detail_analyt_mob_prt_63    values(63),
partition detail_analyt_mob_prt_64    values(64),
partition detail_analyt_mob_prt_65    values(65),
partition detail_analyt_mob_prt_66    values(66),
partition detail_analyt_mob_prt_67    values(67),
partition detail_analyt_mob_prt_68    values(68),
partition detail_analyt_mob_prt_69    values(69),
partition detail_analyt_mob_prt_70    values(70),
partition detail_analyt_mob_prt_71    values(71),
partition detail_analyt_mob_prt_72    values(72),
partition detail_analyt_mob_prt_73    values(73),
partition detail_analyt_mob_prt_74    values(74),
partition detail_analyt_mob_prt_75    values(75),
partition detail_analyt_mob_prt_76    values(76),
partition detail_analyt_mob_prt_77    values(77),
partition detail_analyt_mob_prt_78    values(78),
partition detail_analyt_mob_prt_79    values(79),
partition detail_analyt_mob_prt_80    values(80),
partition detail_analyt_mob_prt_81    values(81),
partition detail_analyt_mob_prt_82    values(82),
partition detail_analyt_mob_prt_83    values(83),
partition detail_analyt_mob_prt_84    values(84),
partition detail_analyt_mob_prt_85    values(85),
partition detail_analyt_mob_prt_86    values(86),
partition detail_analyt_mob_prt_87    values(87),
partition detail_analyt_mob_prt_88    values(88),
partition detail_analyt_mob_prt_89    values(89),
partition detail_analyt_mob_prt_90    values(90),
partition detail_analyt_mob_prt_91    values(91),
partition detail_analyt_mob_prt_92    values(92),
partition detail_analyt_mob_prt_93    values(93),
partition detail_analyt_mob_prt_94    values(94),
partition detail_analyt_mob_prt_95    values(95),
partition detail_analyt_mob_prt_96    values(96),
partition detail_analyt_mob_prt_97    values(97),
partition detail_analyt_mob_prt_98    values(98),
partition detail_analyt_mob_prt_99    values(99),
partition detail_analyt_mob_prt_100    values(100),
DEFAULT PARTITION detail_analyt_mob_prt_default
)
