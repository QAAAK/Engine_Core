CREATE EXTERNAL TABLE dds.hive_spravochnik_tr (
	region_id text,
	region text,
	reg_week2 text,
	tr text
)
LOCATION (
	'pxf://analyticsb2b_sb.rkahmetov_sprav_tr?PROFILE=hive:orc&SERVER=rndDWHB2B'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';