# Severe Weather Database

## Cloud database of severe storms in U.S. from 1950 - Present

### Background

### Data Model

### Architecture

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Final/imgs/architecture_diagram.png?raw=true)<br>

### ELT Pipelines Overview:

There are three general processes that have been tailored to the NOAA's release cadence for severe weather data record files.

The first is the most straightforward - just ingest all of the available details and fatalities csv.gz files available so at present.

To keep the database in sync with the latest data available, we must ingest new data as it is released.  This occurs in two primary ways:

1) Each month, the filename

&emsp;&emsp;1) Initial Load (extract logic in scripts/initial_files_to_blob.py)<br>
&emsp;&emsp;&emsp;&emsp;Immediately triggered load of all available files at source url.<br>
&emsp;&emsp;2) Monthly Update (extract logic in scripts/update_files_to_blob_only.py)<br>
&emsp;&emsp;&emsp;&emsp;ELT the current year's source data file, which is updated monthly.<br>
&emsp;&emsp;3) Yearly New (extract logic in scripts/new_files_to_blob_only.py)<br>
&emsp;&emsp;&emsp;&emsp;ELT the new year's source data file, which lands fresh at source url annually<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Final/imgs/annotated_pull_new_w_id.png?raw=true)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/yearly_deepdive.png?raw=true)<br>

&emsp;&emsp;-Pipelines include handling for Azure Data Lake blob container maintenaince:<br>
&emsp;&emsp;(maintenance logic captured in scripts/datalake_housekeeping.py)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/clean_containers_output.png?raw=true)<br>

&emsp;&emsp;-Testing: 8 total tests:<br>
&emsp;&emsp;&emsp;&emsp;2 tests [1 Extraction x 2 Tables] to confirm each year since 1950 is accounted for with a csvgz file in Data Lake<br>
&emsp;&emsp;&emsp;&emsp;6 tests [3 Pipelines  x 2 Tables] to confirm all rows in source csvgz file is accounted for with a row in MySQL table<br>
&emsp;&emsp;&emsp;&emsp;(see: testing/testing_plan_all_pipelines.txt)<br>

&emsp;&emsp;-All tests pass:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/pipeline_test_success.png?raw=true)

&emsp;&emsp;- Available to explore -> 1.8M rows capturing 70 years of weather data across 62 columns:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/mysqlworkbench_detdate.png?raw=true)
