\c 10 3000
detfiles:asc hsym each `$' ":/home/conner/SevereWeatherDB/data/unzipped/",/:  system "ls ../data/unzipped | grep storm_details"
//locfiles:asc hsym each `$'system "ls ../data/unzipped | grep storm_locations"
fatfiles:asc hsym each `$' ":/home/conner/SevereWeatherDB/data/unzipped/",/:  system "ls ../data/unzipped | grep storm_fatalities"


detorig: (,/) {(51#"*";enlist ",") 0:x} each detfiles
//loc: (,/) {(11#"*";enlist ",") 0:x} each locfiles
fatorig: (,/) {(11#"*";enlist ",") 0:x} each fatfiles

det:detorig
fat:fatorig

update BEGIN_DATE:(BEGIN_YEARMONTH,'BEGIN_DAY) from `det where not 1=count each BEGIN_DAY
update BEGIN_DATE:(BEGIN_YEARMONTH,'("0",'BEGIN_DAY)) from `det where 1=count each BEGIN_DAY
update END_DATE:(END_YEARMONTH,'END_DAY) from `det where not 1=count each END_DAY
update END_DATE:(END_YEARMONTH,'("0",'END_DAY)) from `det where 1=count each END_DAY
update BEGIN_DATETIME:"Z"$((BEGIN_DATE),'(" ",'-8#'BEGIN_DATE_TIME)) from `det
update END_DATETIME:"Z"$((END_DATE),'(" ",'-8#'END_DATE_TIME)) from `det

update "D"$10#'FATALITY_DATE from `fat

//update "M"$YEARMONTH from loc

update "I"$EPISODE_ID,"I"$EVENT_ID,`$STATE,"I"$INJURIES_DIRECT,"I"$INJURIES_INDIRECT,"I"$DEATHS_DIRECT,"I"$DEATHS_INDIRECT,"F"$MAGNITUDE,`$MAGNITUDE_TYPE,"I"$CATEGORY,`$TOR_F_SCALE,"F"$TOR_LENGTH,"F"$TOR_WIDTH,"F"$BEGIN_LAT,"F"$BEGIN_LON,"F"$END_LAT,"F"$END_LON,`$DATA_SOURCE from `det
//update "I"$EPISODE_ID,"I"$EVENT_ID,"I"$LOCATION_INDEX,"F"$RANGE,`$AZIMUTH,`$LOCATION,"F"$LATITUDE,"F"$LONGITUDE,"I"$LAT2,"I"$LON2 from `loc
update "I"$FATALITY_ID,"I"$EVENT_ID,`$FATALITY_TYPE,"I"$FATALITY_AGE,`$FATALITY_SEX from `fat

fat:fat lj (`EVENT_ID xkey select EPISODE_ID,EVENT_ID from det)

sdet: select BEGIN_DATETIME,END_DATETIME,EPISODE_ID,EVENT_ID,STATE,EVENT_TYPE,CZ_NAME,INJ_DIR:INJURIES_DIRECT,INJ_IND:INJURIES_INDIRECT,DEAD_DIR:DEATHS_DIRECT,DEAD_IND:DEATHS_INDIRECT,
  DAMAGE_PROPERTY,DAMAGE_CROPS,SOURCE,BEGIN_LAT,BEGIN_LON,END_LAT,END_LON,EPISODE_NARRATIVE,EVENT_NARRATIVE from det
//sloc: select EPISODE_ID,EVENT_ID,LOCATION,LATITUDE,LONGITUDE from loc
sfat: select FATALITY_DATE,FATALITY_ID,EVENT_ID,EPISODE_ID,FATALITY_TYPE,FATALITY_AGE,FATALITY_SEX,FATALITY_LOCATION from fat

//allmatch:(sdet ij `EPISODE_ID xkey sloc) ij `EPISODE_ID xkey sfat
//allfull:(sdet lj `EPISODE_ID xkey sloc) lj `EPISODE_ID xkey sfat
allmatch:sdet ij `EPISODE_ID xkey sfat
allfull:sdet lj `EPISODE_ID xkey sfat

compmatch:?[`allmatch;not,'enlist each @[(null;0=count');"C"=exec t from meta allmatch],'cols allmatch;0b;()]
compmatch:`BEGIN_DATETIME`EPISODE_ID`EVENT_ID xdesc compmatch

//save `:compmatch.csv

randeps:desc 10?count select i from compmatch where i = (min;i) fby EPISODE_ID
randfull:select from compmatch where i in randeps
//save `:randfull.csv

//THE BEGIN_YEARMONTH, END_YEARMONTH cols make the YEAR, MONTH_NAME cols entirely redundant with the exception of a tiny fraction of cases,
//WHERE THEY DISAGREE WITH END YEAR/MONTH, BUT AGREE WITH BEGIN YEAR, OR DISAGREE WITH BEGIN YEAR, BUT AGREE WITH END YEAR/MONTH.
/
q)update MONTH_NUM:md[MONTH_NAME] from `det
`det
q)update MID_YEARMONTH:(YEAR,'MONTH_NUM) from `det
`det
q)count select from det where {(x = x) and (y < z)}'[("I"$4_'BEGIN_YEARMONTH);"I"$(2#'MONTH_NUM);"I"$(4_'END_YEARMONTH)]
15
q)count select from det where {(y > x) or (y < z)}'[("I"$4#'BEGIN_YEARMONTH);"I"$(4#'YEAR);"I"$(4#'END_YEARMONTH)]
42
q)count select from det where {x <> y}'["I"$END_YEARMONTH;"I"$MID_YEARMONTH]
49
q)count select from det where {x <> y}'["I"$BEGIN_YEARMONTH;"I"$MID_YEARMONTH]
8
q)count select from det where {not y in (x;z)}'[("I"$4_'BEGIN_YEARMONTH);"I"$(2#'MONTH_NUM);"I"$(4_'END_YEARMONTH)]
0
q)count det
1680127
\
