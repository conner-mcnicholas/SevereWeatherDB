source(output(
		BEGIN_YEARMONTH as string,
		BEGIN_DAY as string,
		BEGIN_TIME as string,
		END_YEARMONTH as string,
		END_DAY as string,
		END_TIME as string,
		EPISODE_ID as integer,
		EVENT_ID as integer,
		STATE as string,
		STATE_FIPS as integer,
		YEAR as integer,
		MONTH_NAME as string,
		EVENT_TYPE as string,
		CZ_TYPE as string,
		CZ_FIPS as integer,
		CZ_NAME as string,
		WFO as string,
		BEGIN_DATE_TIME as string,
		CZ_TIMEZONE as string,
		END_DATE_TIME as string,
		INJURIES_DIRECT as integer,
		INJURIES_INDIRECT as integer,
		DEATHS_DIRECT as integer,
		DEATHS_INDIRECT as integer,
		DAMAGE_PROPERTY as string,
		DAMAGE_CROPS as string,
		SOURCE as string,
		MAGNITUDE as decimal(9,2),
		MAGNITUDE_TYPE as string,
		FLOOD_CAUSE as string,
		CATEGORY as integer,
		TOR_F_SCALE as string,
		TOR_LENGTH as decimal(9,2),
		TOR_WIDTH as decimal(9,2),
		TOR_OTHER_WFO as string,
		TOR_OTHER_CZ_STATE as string,
		TOR_OTHER_CZ_FIPS as integer,
		TOR_OTHER_CZ_NAME as string,
		BEGIN_RANGE as integer,
		BEGIN_AZIMUTH as string,
		BEGIN_LOCATION as string,
		END_RANGE as integer,
		END_AZIMUTH as string,
		END_LOCATION as string,
		BEGIN_LAT as decimal(9,4),
		BEGIN_LON as decimal(9,4),
		END_LAT as decimal(9,4),
		END_LON as decimal(9,4),
		EPISODE_NARRATIVE as string,
		EVENT_NARRATIVE as string,
		DATA_SOURCE as string
	),
	allowSchemaDrift: false,
	validateSchema: false,
	fileList: true,
	isolationLevel: 'READ_UNCOMMITTED',
	format: 'table') ~> source1
source1 alterRow(upsertIf(true())) ~> AlterRow1
AlterRow1 sink(allowSchemaDrift: false,
	validateSchema: false,
	input(
		BEGIN_YEARMONTH as string,
		BEGIN_DAY as string,
		BEGIN_TIME as string,
		END_YEARMONTH as string,
		END_DAY as string,
		END_TIME as string,
		EPISODE_ID as integer,
		EVENT_ID as integer,
		STATE as string,
		STATE_FIPS as integer,
		YEAR as integer,
		MONTH_NAME as string,
		EVENT_TYPE as string,
		CZ_TYPE as string,
		CZ_FIPS as integer,
		CZ_NAME as string,
		WFO as string,
		BEGIN_DATE_TIME as string,
		CZ_TIMEZONE as string,
		END_DATE_TIME as string,
		INJURIES_DIRECT as integer,
		INJURIES_INDIRECT as integer,
		DEATHS_DIRECT as integer,
		DEATHS_INDIRECT as integer,
		DAMAGE_PROPERTY as string,
		DAMAGE_CROPS as string,
		SOURCE as string,
		MAGNITUDE as decimal(9,2),
		MAGNITUDE_TYPE as string,
		FLOOD_CAUSE as string,
		CATEGORY as integer,
		TOR_F_SCALE as string,
		TOR_LENGTH as decimal(9,2),
		TOR_WIDTH as decimal(9,2),
		TOR_OTHER_WFO as string,
		TOR_OTHER_CZ_STATE as string,
		TOR_OTHER_CZ_FIPS as integer,
		TOR_OTHER_CZ_NAME as string,
		BEGIN_RANGE as integer,
		BEGIN_AZIMUTH as string,
		BEGIN_LOCATION as string,
		END_RANGE as integer,
		END_AZIMUTH as string,
		END_LOCATION as string,
		BEGIN_LAT as decimal(9,4),
		BEGIN_LON as decimal(9,4),
		END_LAT as decimal(9,4),
		END_LON as decimal(9,4),
		EPISODE_NARRATIVE as string,
		EVENT_NARRATIVE as string,
		DATA_SOURCE as string
	),
	deletable:false,
	insertable:false,
	updateable:false,
	upsertable:true,
	keys:['EVENT_ID'],
	format: 'table',
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	mapColumn(
		BEGIN_YEARMONTH,
		BEGIN_DAY,
		BEGIN_TIME,
		END_YEARMONTH,
		END_DAY,
		END_TIME,
		EPISODE_ID,
		EVENT_ID,
		STATE,
		STATE_FIPS,
		YEAR,
		MONTH_NAME,
		EVENT_TYPE,
		CZ_TYPE,
		CZ_FIPS,
		CZ_NAME,
		WFO,
		BEGIN_DATE_TIME,
		CZ_TIMEZONE,
		END_DATE_TIME,
		INJURIES_DIRECT,
		INJURIES_INDIRECT,
		DEATHS_DIRECT,
		DEATHS_INDIRECT,
		DAMAGE_PROPERTY,
		DAMAGE_CROPS,
		SOURCE,
		MAGNITUDE,
		MAGNITUDE_TYPE,
		FLOOD_CAUSE,
		CATEGORY,
		TOR_F_SCALE,
		TOR_LENGTH,
		TOR_WIDTH,
		TOR_OTHER_WFO,
		TOR_OTHER_CZ_STATE,
		TOR_OTHER_CZ_FIPS,
		TOR_OTHER_CZ_NAME,
		BEGIN_RANGE,
		BEGIN_AZIMUTH,
		BEGIN_LOCATION,
		END_RANGE,
		END_AZIMUTH,
		END_LOCATION,
		BEGIN_LAT,
		BEGIN_LON,
		END_LAT,
		END_LON,
		EPISODE_NARRATIVE,
		EVENT_NARRATIVE,
		DATA_SOURCE
	)) ~> sink1