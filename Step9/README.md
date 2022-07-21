# Severe Weather Database

## Cloud database of severe storms in U.S. from 1950 - Present

### Current State

Step 9 - Deploy Code to Production:<br>
&emsp;&emsp;-Prior steps outlined as slide-deck PDF in respective folders<br>
&emsp;&emsp;-Architecture:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step7/diagram_formats/final_archdiagram_mod.png?raw=true)

&emsp;&emsp;-Packaged scripts as docker container to auto-deploy all required Azure Resources based on config:<br>

&emsp;&emsp;(see: AZURE_resources/README.md)<br>

&emsp;&emsp;-The success up to this stage makes it reasonable to finally load all files
&emsp;&emsp;&emsp;&emsp;-From earliest records in 1950-Present (previously had only loaded files for years > 2000)

&emsp;&emsp;-Deployed all logic within 3 pipelines in Azure Data Factory and scheduled triggers:<br>
&emsp;&emsp;&emsp;1) Initial Load: <br>
&emsp;&emsp;&emsp;(data extract logic captured in scripts/initial_files_to_blob.py)<br>
&emsp;&emsp;&emsp;(previously implemented as ../Step8/batch_upload_to_blob.py)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/initial_load.png?raw=true)<br>

&emsp;&emsp;&emsp;2) Monthly Update:<br>
&emsp;&emsp;&emsp;(data extract logic captured in scripts/update_files_to_blob.py)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/monthly_update.png?raw=true)<br>

&emsp;&emsp;&emsp;3) Yearly New:<br>
&emsp;&emsp;&emsp;(data extract logic captured in scripts/new_files_to_blob.py)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/yearly_new.png?raw=true)<br>

&emsp;&emsp;-Pipelines include handling for Azure Data Lake blob container maintenaince:<br>
&emsp;&emsp;(maintenance logic captured in scripts/datalake_housekeeping.py)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/clean_containers_output.png?raw=true)<br>

&emsp;&emsp;-Update and New ETL Pipelines covered by 4 additional test cases (2 pipelines x 2 tables - details + fatalities):<br>
&emsp;&emsp;&emsp;&emsp;-This is in addition to being successfully run through ADF debug trigger.<br>
&emsp;&emsp;(initial ETL vetted in: ../Step8/runtests.py)<br>
&emsp;&emsp;(see: testing/testing_plan_all_pipelines.txt)<br>

&emsp;&emsp;-All tests pass:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/pipeline_test_success.png?raw=true)

&emsp;&emsp;- Available to explore -> 1.5M rows capturing 21st century weather data across 62 columns:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/mysqlworkbench_detdate.png?raw=true)

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/sanddance.png?raw=true)
