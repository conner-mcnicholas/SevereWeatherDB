# Severe Weather Database

## Cloud database of 21st Century severe US storms

### Current State

Step 9 - Deploy Code to Production:<br>
&emsp;&emsp;-Prior steps outlined as slide-deck PDF in respective folders<br>
&emsp;&emsp;-Architecture:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step7/diagram_formats/final_archdiagram_mod.png?raw=true)

&emsp;&emsp;-Packaged scripts as docker container to auto-deploy all required Azure Resources based on config:<br>

&emsp;&emsp;(see: AZURE_resources/CLI-resource_creator)<br>

&emsp;&emsp;-Deployed all logic as pipelines in Azure Data Factory and scheduled triggers:<br>

&emsp;&emsp;(see: overview_all_pipelines.txt)<br>

&emsp;&emsp;&emsp;1) Initial Pipeline - One time ingestion of all available data past Start Year = 2000: <br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/full_init_pipeline.png?raw=true)<br>

&emsp;&emsp;&emsp;2) Update Pipeline - Runs monthly, ingests updated file with extra rows added for current year+month :<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/full_update_pipeline.png?raw=true)<br>

&emsp;&emsp;&emsp;2) New Pipeline - Runs annually, ingests new file with all new data for current year:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/full_new_pipeline.png?raw=true)<br>

&emsp;&emsp;Pipelines include handling for Azure Data Lake blob container maintenaince:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/clean_containers_output.png?raw=true)<br>

&emsp;&emsp;-Update and New ETL Pipelines covered by 4 additional test cases (2 pipelines x 2 tables - details + fatalities):<br>
&emsp;&emsp;&emsp;1 test to verify that updated file pipeline ingests all additonal rows from updated file <br>
&emsp;&emsp;&emsp;1 test to verify that new file pipeline ingests all rows from new file <br>

&emsp;&emsp;-All tests pass:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step9/imgs/pipeline_test_success.png?raw=true)

&emsp;&emsp;- Available to explore -> 1.5M rows capturing 21st century weather data across 62 columns:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/mysqlworkbench_detdate.png?raw=true)

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/sanddance.png?raw=true)
