# Severe Weather Database

Cloud database cataloging records of severe storms in the United States dating back to 1970.<br>

Slide decks describing process and results of each step is found as odp and pdf files in respective dirs.<br>

Current State - Step 8 (Deploy Your Code for Testing):<br>
&emsp;&emsp;-Changed course on tech stack (see Step7/DeploymentArchitecture.pdf):<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step7/diagram_formats/final_archdiagram_mod.png?raw=true)

&emsp;&emsp;-Wrote 4 test cases (2 tests x 2 tables - details + fatalities):<br>
&emsp;&emsp;&emsp;&emsp;-1 test to verify that all available files from source have been uploaded to blob<br>
&emsp;&emsp;&emsp;&emsp;-1 test to verify that every line from source files have been copied over to MySQL tables<br>

&emsp;&emsp;-All tests pass, as captured in Step8/test_results.out:<br>

![alt text](https://github.com/conner-mcnicholas/SevereWeatherDB/blob/main/Step8/imgs/all_tests_pass.png?raw=true)
