# Severe Weather Database

## Cloud database of 21st Century severe US storms

### Current State

Step 8 - Deploy Code for Testing:<br>
&emsp;&emsp;-prior steps outlined as slide-deck PDF in respective folders<br>
&emsp;&emsp;-Changed course on tech stack (see Step7/DeploymentArchitecture.pdf):<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step7/diagram_formats/final_archdiagram_mod.png?raw=true)

&emsp;&emsp;-There were 2 steps in the ETL Processs:<br>
&emsp;&emsp;&emsp;1) Run python script to batch upload all from 2000-Present to Azure Data Lake containers: <br>

`python uploadtoblob.py 2000`

&emsp;&emsp;&emsp;2) Run pipelines in Azure Data Factory to copy data from blob input to MySQL table output:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/DATASET_input_details_gz_cont.png?raw=true)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/CP_details_gz_cont.png?raw=true)<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/data_factory_successful_cp.png?raw=true)<br>

&emsp;&emsp;-Entire ETL is covered by 4 test cases (2 tests x 2 tables - details + fatalities):<br>
&emsp;&emsp;&emsp;1 test to verify that all csv.gz files from source have been uploaded to blob<br>
&emsp;&emsp;&emsp;1 test to verify that every line from source files was copied to MySQL tables<br>

&emsp;&emsp;-All tests pass, as captured in Step8/test_results.out:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/all_tests_pass.png?raw=true)

&emsp;&emsp;- Available to explore -> 1.5M rows capturing 21st century weather data across 62 columns:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/mysqlworkbench_detdate.png?raw=true)

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/sanddance.png?raw=true)
