# Loading data from AppDynamics (RestAPI) To a BigQuery table.
![Description](https://www.logolynx.com/images/logolynx/56/56eb806e53749836ddfa028ef4a0fe00.png)

---
### Table of Contents
Following steps describe over all architecture to pull data from AppDynamics (Rest-API) and load it to a BigQuery Table.

- [1. Master Script](##master-script)
- [2. Data Gen Script](#data-gen)
- [3. Data Loading Script](#data-loading)

---

  ## 1. Master Script 

> This is master script which is responsible for Pulling data from Rest-API and load it into an intermediate file. Then validation of record count is done, if any deviation then alarm is raised else complete data is considered for loading. Post data loading through Apache beam script deletion of intermediate files are done.!

We can find Master shell script Code [SH_Gen_60Min_Data.sh](https://github.com/vibwipro/AppDynamics-RestAPI-To-BigQuery/blob/main/SH_Gen_60Min_Data.sh)

---
---

  ## 2. Data Generator(Pulling) Script

> This is data generator or pulling script which is responsible for Pulling data from AppDynamics Rest-API that will be considered for loading. This script take input file name and server name for which data need to be pulled from Rest-API. Different metric data are been pulled from Rest-API and written to a file for loading. Script is also responsible for generating token key for every run and it will help in authenticating connection.

We can find Data Gen Python Code [Gen_60Min_rollup.py](https://github.com/vibwipro/AppDynamics-RestAPI-To-BigQuery/blob/main/Gen_60Min_rollup.py)

---
---

  ## 3. Data Loading (Apache Beam) Script

> This is data loading (Apache Beam) script which is responsible for loading data to a BigQuery table, same data which we have puled from AppDynamics Rest-API. This script take input file name that is holding data which need to be loaded over BigQuery tables. Different metric data are loaded to different BQ tables bases on filter conditions.

We can find Data Loading Python Code [BQ_Load_60Min_rollup.py](https://github.com/vibwipro/AppDynamics-RestAPI-To-BigQuery/blob/main/BQ_Load_60Min_rollup.py)

---
