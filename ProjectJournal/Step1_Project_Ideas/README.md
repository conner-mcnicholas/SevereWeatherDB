# Capstone Ideas

DOC/NOAA/NESDIS/NCEI > National Centers for Environmental Information, NESDIS, NOAA, U.S. Department of Commerce

The National Oceanic and Atmospheric Administration (NOAA)
publicly launched on April 22, 2015
the National Centers for Environmental Information (NCEI) by merging
NOAA's National Climatic Data Center (NCDC),
National Geophysical Data Center (NGDC), and
National Oceanographic Data Center (NODC), including the
National Coastal Data Development Center (NCDDC)

this government org has TONS of well curated and interesting datasets:
https://www.ncei.noaa.gov/access/search/dataset-search

Among those that lend themselves to data engineering/elt apps:
NCDC Storm Events Database:
https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00510

The Storm Events Database contains the records used to create the official NOAA Storm Data publication, documenting:

-The occurrence of storms and other significant weather phenomena having sufficient intensity to cause loss of life, injuries, significant property damage, and/or disruption to commerce;
-Rare, unusual, weather phenomena that generate media attention, such as snow flurries in South Florida or the San Diego coastal area; and
-Other significant meteorological events, such as record maximum or minimum temperatures or precipitation that occur in connection with another event.

The database currently contains data from January 1950 to April 2022, as entered by NOAA's National Weather Service (NWS). Due to changes in the data collection and processing procedures over time, there are unique periods of record available depending on the event type. NCEI has performed data reformatting and standardization of event types but has not changed any data values for locations, fatalities, injuries, damage, narratives and any other event specific information.

Storm Data is provided by the National Weather Service (NWS) and contain statistics on personal injuries and damage estimates. Storm Data covers the United States of America. The data began as early as 1950 through to the present, updated monthly with up to a 120 day delay possible. NCDC Storm Event database allows users to find various types of storms recorded by county, or use other selection criteria as desired. The data contain a chronological listing, by state, of hurricanes, tornadoes, thunderstorms, hail, floods, drought conditions, lightning, high winds, snow, temperature extremes and other weather phenomena.

Purpose:	To make a wide range of storm event data available to researchers and the public.
Status: Ongoing
UpdateFreq:	Monthly
Edition:	v1.0
Format:CSV
fields:https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/Storm-Data-Bulk-csv-Format.pdf
http:https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
ftp:ftp://ftp.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
FAQ:https://www.ncdc.noaa.gov/stormevents/faq.jsp
