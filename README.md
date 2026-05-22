Milestone 2: Chicago Crime Analytics with Spark + MLlib

Team members: Mohamad Al Deri 230151, Abdulaziz Altamimi 230714

Executive Summary
In Milestone 2, our team successfully migrated our batch-processing MapReduce pipeline to an in-memory Apache Spark architecture. We reproduced our analytical queries using Spark DataFrames and Spark SQL, achieving significantly faster execution times. Furthermore, we built an end-to-end Machine Learning pipeline using Spark MLlib, testing three distinct classification algorithms to predict whether a reported crime would result in an arrest based on location, time, and crime type.
Phase A: M1 vs M2 Comparison (MapReduce vs Spark)
Task	MapReduce Result (M1)	Spark Result (M2)	Comparison Notes
Task 1: Crime Types	THEFT (162,688)	THEFT (162,688)	Identical.
	BATTERY (151,930)	BATTERY (151,930)	Identical.
	CRIM. DAMAGE (91,241)	CRIM. DAMAGE (91,241)	Identical.
Task 2: Hotspots	STREET (245,437)	STREET (248,326)	Spark parsed more valid rows.
	RESIDENCE (136,238)	RESIDENCE (136,393)	Slight increase in Spark count.
	APARTMENT (60,925)	APARTMENT (61,235)	Spark handled nested quotes better.
Task 3: Trend	2001 (467,301)	2001 (467,301)	Perfect match.
	2002 (205,267)	2002 (205,266)	Negligible difference (1 row).
	2023 (8,146)	2023 (81,461)	Note: Ensure year extraction logic is consistent.
Task 4: Arrests	Total: ~28% Arrest Rate	Total: 28% Arrest Rate	Spark is more efficient to aggregate.
	Narcotics: 100%	Narcotics: 100%	Both identify high-arrest crimes.
	Deceptive Prac: ~23%	Deceptive Prac: 23%	Both identify low-arrest crimes.

 


Phase B: ML Results Summary
Author: Mohamad Al Deri (ID: 230151)
To predict whether a crime results in an arrest, we engineered four features: District, crime_index from Primary Type, Hour, and domestic_index. We trained and evaluated three models on a 5% sample of the dataset to accommodate cluster memory limits.
Model Comparison: Logistic Regression: AUC-ROC 0.5717, Accuracy 0.5647, F1 Score 0.5011, Precision 0.4828, Recall 0.5647, Training Time 2.39s Random Forest: AUC-ROC 0.8088, Accuracy 0.7647, F1 Score 0.7629, Precision 0.7620, Recall 0.7647, Training Time 5.44s GBT: AUC-ROC 0.7509, Accuracy 0.7059, F1 Score 0.7048, Precision 0.7040, Recall 0.7059, Training Time 27.74s
 Model Recommendation & Interpretation: Random Forest is our recommended model. It outperformed the other models across the board, achieving the highest Accuracy 76.47% and the best overall AUC-ROC 0.8088, meaning it is the most reliable at distinguishing between arrests and non-arrests. Furthermore, Random Forest trained five times faster than GBT 5.44s vs 27.74s, making it the most efficient choice for scaling up to the full 7-million row dataset.
Logistic Regression struggled massively on this dataset, dropping to an accuracy of just 56.47% (barely better than random guessing. This proves that the relationship between our features and an arrest is highly non-linear. Logistic Regression attempts to treat our categorical crime_index as a continuous mathematical slope, whereas the tree-based models branch on specific categorical values.
Feature Importance: Extracting the decision weights from our Random Forest model reveals exactly how the algorithm makes its predictions:
1.	Crime Type (crime_index): 77.98%
2.	Location (District): 10.15%
3.	Time (Hour): 9.38%
4.	Domestic Status (domestic_index): 2.48%
 


Analysis: The Primary Type of crime remains the strongest predictor of an arrest by a wide margin nearly 78%. However, unlike our previous test, District 10.1% and Hour 9.3% also show up as meaningful factors. This indicates that while the inherent nature of the crime heavily dictates the arrest probability, where and when the crime occurs also influences police response and arrest outcomes.

Phase C: Deployment Evidence
 
 



Task 10 Terminal Logs:

PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> scp M2_Spark_ML_Group_Altamimi_AlDeri.py aoaltamimi@134.209.172.50:~/
aoaltamimi@134.209.172.50's password: 
M2_Spark_ML_Group_Altamimi_AlDeri.py                                                                                                                                                                                    100%   14KB  82.6KB/s   00:00    
PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> ssh aoaltamimi@134.209.172.50                                        
aoaltamimi@134.209.172.50's password: 
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-176-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Fri May 22 16:13:33 UTC 2026

Expanded Security Maintenance for Applications is not enabled.

33 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


*** System restart required ***
Last login: Fri May 22 16:09:56 2026 from 5.163.250.180
aoaltamimi@master-node:~$ spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "SE446-M2-Task10" \
  M2_Spark_ML_Group_Altamimi_AlDeri.py
26/05/22 16:13:58 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Running in CLUSTER mode
Python executable: /usr/bin/python3.12
26/05/22 16:14:00 INFO SparkContext: Running Spark version 3.5.4
26/05/22 16:14:00 INFO SparkContext: OS info Linux, 5.15.0-176-generic, amd64
26/05/22 16:14:00 INFO SparkContext: Java version 11.0.30
26/05/22 16:14:00 INFO ResourceUtils: ==============================================================
26/05/22 16:14:00 INFO ResourceUtils: No custom resources configured for spark.driver.
26/05/22 16:14:00 INFO ResourceUtils: ==============================================================
26/05/22 16:14:00 INFO SparkContext: Submitted application: SE446-M2-PhaseA-Altamimi-AlDeri
26/05/22 16:14:00 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 768, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
26/05/22 16:14:00 INFO ResourceProfile: Limiting resource is cpus at 1 tasks per executor
26/05/22 16:14:00 INFO ResourceProfileManager: Added ResourceProfile id: 0
26/05/22 16:14:00 INFO SecurityManager: Changing view acls to: aoaltamimi
26/05/22 16:14:00 INFO SecurityManager: Changing modify acls to: aoaltamimi
26/05/22 16:14:00 INFO SecurityManager: Changing view acls groups to: 
26/05/22 16:14:00 INFO SecurityManager: Changing modify acls groups to: 
26/05/22 16:14:00 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: aoaltamimi; groups with view permissions: EMPTY; users with modify permissions: aoaltamimi; groups with modify permissions: EMPTY
26/05/22 16:14:01 INFO Utils: Successfully started service 'sparkDriver' on port 36485.
26/05/22 16:14:01 INFO SparkEnv: Registering MapOutputTracker
26/05/22 16:14:01 INFO SparkEnv: Registering BlockManagerMaster
26/05/22 16:14:01 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
26/05/22 16:14:01 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
26/05/22 16:14:01 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
26/05/22 16:14:01 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-2b3863ad-069a-46e9-9bea-902cb0b7611b
26/05/22 16:14:01 INFO MemoryStore: MemoryStore started with capacity 127.2 MiB
26/05/22 16:14:01 INFO SparkEnv: Registering OutputCommitCoordinator
26/05/22 16:14:01 INFO JettyUtils: Start Jetty 0.0.0.0:4040 for SparkUI
26/05/22 16:14:01 INFO Utils: Successfully started service 'SparkUI' on port 4040.
26/05/22 16:14:01 INFO SparkContext: Added JAR file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/commons-pool2-2.12.0.jar at spark://master-node:36485/jars/commons-pool2-2.12.0.jar with timestamp 1779466440118
26/05/22 16:14:01 INFO SparkContext: Added JAR file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/kafka-clients-3.9.0.jar at spark://master-node:36485/jars/kafka-clients-3.9.0.jar with timestamp 1779466440118
26/05/22 16:14:01 INFO SparkContext: Added JAR file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-sql-kafka-0-10_2.12-3.5.4.jar at spark://master-node:36485/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar with timestamp 1779466440118
26/05/22 16:14:01 INFO SparkContext: Added JAR file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-token-provider-kafka-0-10_2.12-3.5.4.jar at spark://master-node:36485/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar with timestamp 1779466440118
26/05/22 16:14:02 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
26/05/22 16:14:03 INFO Configuration: resource-types.xml not found
26/05/22 16:14:03 INFO ResourceUtils: Unable to find 'resource-types.xml'.
26/05/22 16:14:03 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (1536 MB per container)
26/05/22 16:14:03 INFO Client: Will allocate AM container, with 640 MB memory including 384 MB overhead
26/05/22 16:14:03 INFO Client: Setting up container launch context for our AM
26/05/22 16:14:03 INFO Client: Setting up the launch environment for our AM container
26/05/22 16:14:03 INFO Client: Preparing resources for our AM container
26/05/22 16:14:03 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/commons-pool2-2.12.0.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/commons-pool2-2.12.0.jar
26/05/22 16:14:04 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/kafka-clients-3.9.0.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/kafka-clients-3.9.0.jar
26/05/22 16:14:05 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-sql-kafka-0-10_2.12-3.5.4.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/spark-sql-kafka-0-10_2.12-3.5.4.jar
26/05/22 16:14:06 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-token-provider-kafka-0-10_2.12-3.5.4.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/spark-token-provider-kafka-0-10_2.12-3.5.4.jar
26/05/22 16:14:06 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/python/lib/pyspark.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/pyspark.zip
26/05/22 16:14:07 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/py4j-0.10.9.7-src.zip
26/05/22 16:14:08 INFO Client: Uploading resource file:/tmp/spark-0f61dc4c-36f1-41dd-850c-ba8eaf31496f/__spark_conf__11078375432782058572.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0103/__spark_conf__.zip
26/05/22 16:14:09 INFO SecurityManager: Changing view acls to: aoaltamimi
26/05/22 16:14:09 INFO SecurityManager: Changing modify acls to: aoaltamimi
26/05/22 16:14:09 INFO SecurityManager: Changing view acls groups to: 
26/05/22 16:14:09 INFO SecurityManager: Changing modify acls groups to: 
26/05/22 16:14:09 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: aoaltamimi; groups with view permissions: EMPTY; users with modify permissions: aoaltamimi; groups with modify permissions: EMPTY
26/05/22 16:14:09 INFO Client: Submitting application application_1778738889964_0103 to ResourceManager
26/05/22 16:14:09 INFO YarnClientImpl: Submitted application application_1778738889964_0103
26/05/22 16:14:10 INFO Client: Application report for application_1778738889964_0103 (state: ACCEPTED)
26/05/22 16:14:10 INFO Client: 
         client token: N/A
         diagnostics: AM container is launched, waiting for AM container to Register with RM
         ApplicationMaster host: N/A
         ApplicationMaster RPC port: -1
         queue: root.default
         start time: 1779466449353
         final status: UNDEFINED
         tracking URL: http://master-node:8088/proxy/application_1778738889964_0103/
         user: aoaltamimi
26/05/22 16:14:23 INFO Client: Application report for application_1778738889964_0103 (state: RUNNING)
26/05/22 16:14:23 INFO Client: 
         client token: N/A
         diagnostics: N/A
         ApplicationMaster host: 164.92.103.148
         ApplicationMaster RPC port: -1
         queue: root.default
         start time: 1779466449353
         final status: UNDEFINED
         tracking URL: http://master-node:8088/proxy/application_1778738889964_0103/
         user: aoaltamimi
26/05/22 16:14:23 INFO YarnClientSchedulerBackend: Application application_1778738889964_0103 has started running.
26/05/22 16:14:23 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34205.
26/05/22 16:14:23 INFO NettyBlockTransferService: Server created on master-node:34205
26/05/22 16:14:23 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
26/05/22 16:14:23 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, master-node, 34205, None)
26/05/22 16:14:23 INFO BlockManagerMasterEndpoint: Registering block manager master-node:34205 with 127.2 MiB RAM, BlockManagerId(driver, master-node, 34205, None)
26/05/22 16:14:23 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, master-node, 34205, None)
26/05/22 16:14:23 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, master-node, 34205, None)
26/05/22 16:14:23 INFO YarnClientSchedulerBackend: Add WebUI Filter. org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter, Map(PROXY_HOSTS -> master-node, PROXY_URI_BASES -> http://master-node:8088/proxy/application_1778738889964_0103), /proxy/application_1778738889964_0103
26/05/22 16:14:23 INFO SingleEventLogFileWriter: Logging events to hdfs:/spark-logs/application_1778738889964_0103.inprogress
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /jobs: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /jobs/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /jobs/job: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /jobs/job/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/stage: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/stage/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/pool: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/pool/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /storage: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /storage/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /storage/rdd: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /storage/rdd/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /environment: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /environment/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors/threadDump: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors/threadDump/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors/heapHistogram: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /executors/heapHistogram/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /static: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /api: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /jobs/job/kill: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /stages/stage/kill: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO ServerInfo: Adding filter to /metrics/json: org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter
26/05/22 16:14:24 INFO YarnSchedulerBackend$YarnSchedulerEndpoint: ApplicationMaster registered as NettyRpcEndpointRef(spark-client://YarnAM)
26/05/22 16:14:32 INFO YarnClientSchedulerBackend: SchedulerBackend is ready for scheduling beginning after waiting maxRegisteredResourcesWaitingTime: 30000000000(ns)
Master: yarn
Spark version: 3.5.4
Web UI: http://master-node:4040
Loading data from HDFS: hdfs:///data/chicago_crimes.csv
Total records: 793,073
Schema:
root
 |-- ID: integer (nullable = true)
 |-- Case Number: string (nullable = true)
 |-- Date: string (nullable = true)
 |-- Block: string (nullable = true)
 |-- IUCR: string (nullable = true)
 |-- Primary Type: string (nullable = true)
 |-- Description: string (nullable = true)
 |-- Location Description: string (nullable = true)
 |-- Arrest: boolean (nullable = true)
 |-- Domestic: boolean (nullable = true)
 |-- Beat: integer (nullable = true)
 |-- District: integer (nullable = true)
 |-- Ward: integer (nullable = true)
 |-- Community Area: integer (nullable = true)
 |-- FBI Code: string (nullable = true)
 |-- X Coordinate: integer (nullable = true)
 |-- Y Coordinate: integer (nullable = true)
 |-- Year: integer (nullable = true)
 |-- Updated On: string (nullable = true)
 |-- Latitude: double (nullable = true)
 |-- Longitude: double (nullable = true)
 |-- Location: string (nullable = true)

+--------+-----------+----------------------+-----------------------+----+--------------------------+------------------------------+--------------------------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+----------------------+------------+-------------+-----------------------------+
|ID      |Case Number|Date                  |Block                  |IUCR|Primary Type              |Description                   |Location Description                  |Arrest|Domestic|Beat|District|Ward|Community Area|FBI Code|X Coordinate|Y Coordinate|Year|Updated On            |Latitude    |Longitude    |Location                     |
+--------+-----------+----------------------+-----------------------+----+--------------------------+------------------------------+--------------------------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+----------------------+------------+-------------+-----------------------------+
|13311263|JG503434   |07/29/2022 03:39:00 AM|023XX S TROY ST        |1582|OFFENSE INVOLVING CHILDREN|CHILD PORNOGRAPHY             |RESIDENCE                             |true  |false   |1033|10      |25  |30            |17      |NULL        |NULL        |2022|04/18/2024 03:40:59 PM|NULL        |NULL         |NULL                         |
|13053066|JG103252   |01/03/2023 04:44:00 PM|039XX W WASHINGTON BLVD|2017|NARCOTICS                 |MANUFACTURE / DELIVER - CRACK |SIDEWALK                              |true  |false   |1122|11      |28  |26            |18      |NULL        |NULL        |2023|01/20/2024 03:41:12 PM|NULL        |NULL         |NULL                         |
|12131221|JD327000   |08/10/2020 09:45:00 AM|015XX N DAMEN AVE      |0326|ROBBERY                   |AGGRAVATED VEHICULAR HIJACKING|STREET                                |true  |false   |1424|14      |1   |24            |03      |1162795     |1909900     |2020|05/17/2025 03:40:52 PM|41.908417822|-87.67740693 |(41.908417822, -87.67740693) |
|11227634|JB147599   |08/26/2017 10:00:00 AM|001XX W RANDOLPH ST    |0281|CRIM SEXUAL ASSAULT       |NON-AGGRAVATED                |HOTEL/MOTEL                           |false |false   |122 |1       |42  |32            |02      |NULL        |NULL        |2017|02/11/2018 03:57:41 PM|NULL        |NULL         |NULL                         |
|13203321|JG415333   |09/06/2023 05:00:00 PM|002XX N Wells st       |1320|CRIMINAL DAMAGE           |TO VEHICLE                    |PARKING LOT / GARAGE (NON RESIDENTIAL)|false |false   |122 |1       |42  |32            |14      |1174694     |1901831     |2023|11/04/2023 03:40:18 PM|41.886018055|-87.633937881|(41.886018055, -87.633937881)|
+--------+-----------+----------------------+-----------------------+----+--------------------------+------------------------------+--------------------------------------+------+--------+----+--------+----+--------------+--------+------------+------------+----+----------------------+------------+-------------+-----------------------------+
only showing top 5 rows

TASK 1: Crime Type Distribution (Spark DataFrame)

Top 10 Crime Types by Count:
+-------------------+------+
|Primary Type       |count |
+-------------------+------+
|THEFT              |162688|
|BATTERY            |151930|
|CRIMINAL DAMAGE    |91241 |
|NARCOTICS          |74127 |
|ASSAULT            |54070 |
|MOTOR VEHICLE THEFT|48494 |
|BURGLARY           |39872 |
|OTHER OFFENSE      |36893 |
|ROBBERY            |30991 |
|DECEPTIVE PRACTICE |30396 |
+-------------------+------+
only showing top 10 rows

TASK 2: Location Hotspots (Spark SQL)

Top 10 Crime Location Hotspots:
+--------------------+------+
|Location Description| total|
+--------------------+------+
|              STREET|248326|
|           RESIDENCE|136393|
|           APARTMENT| 61235|
|            SIDEWALK| 47506|
|               OTHER| 29671|
|PARKING LOT/GARAG...| 22436|
|               ALLEY| 18349|
|SCHOOL, PUBLIC, B...| 15776|
|    RESIDENCE-GARAGE| 14291|
|  SMALL RETAIL STORE| 13804|
+--------------------+------+

TASK 3: Crime Trend Over Years

Crime Count Per Year:
  Year  count
   NaN      1
2001.0 467301
2002.0 205266
2003.0    985
2004.0    915
2005.0   1031
2006.0    796
2007.0    762
2008.0   1010
2009.0    910
2010.0    695
2011.0    770
2012.0    800
2013.0    714
2014.0    825
2015.0   1105
2016.0   1339
2017.0   1387
2018.0   1327
2019.0   1174
2020.0   1832
2021.0   2399
2022.0   4678
2023.0  81461
2024.0    880
2025.0  12710
Visualization skipped: matplotlib not available
TASK 4: Arrest Rate Analysis

Overall Statistics:
Total Crimes: 793,073
Total Arrests: 221,932
Arrest Rate: 0.28

Crime Types with Highest Arrest Rates:
+--------------------+------+-------+---------------+
|        Primary Type| total|arrests|arrest_rate_pct|
+--------------------+------+-------+---------------+
|   DOMESTIC VIOLENCE|     1|      1|            1.0|
|            GAMBLING|  1314|   1311|            1.0|
|LIQUOR LAW VIOLATION|  2349|   2345|            1.0|
|    PUBLIC INDECENCY|    17|     17|            1.0|
|        PROSTITUTION|  9100|   9089|            1.0|
|           NARCOTICS| 74127|  74039|            1.0|
|CONCEALED CARRY L...|    77|     73|           0.95|
|OTHER NARCOTIC VI...|    11|     10|           0.91|
|INTERFERENCE WITH...|   803|    648|           0.81|
|   WEAPONS VIOLATION|  8893|   6634|           0.75|
|   CRIMINAL TRESPASS| 21476|  15803|           0.74|
|PUBLIC PEACE VIOL...|  1827|   1221|           0.67|
|            HOMICIDE| 13173|   6338|           0.48|
|           OBSCENITY|    24|     10|           0.42|
|         SEX OFFENSE|  3932|   1273|           0.32|
|OFFENSE INVOLVING...|  2065|    501|           0.24|
| CRIM SEXUAL ASSAULT|  2463|    585|           0.24|
|       OTHER OFFENSE| 36893|   8769|           0.24|
|  DECEPTIVE PRACTICE| 30396|   6918|           0.23|
|             BATTERY|151930|  33102|           0.22|
+--------------------+------+-------+---------------+
only showing top 20 rows

26/05/22 16:15:48 WARN SparkStringUtils: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
Sampled Training set size: 31728 rows
Sampled Testing set size: 7806 rows
Training Logistic Regression...
26/05/22 16:16:28 WARN InstanceBuilder: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
Training Random Forest...
Training GBT...

--- Model Comparison ---
              Model  AUC-ROC  Accuracy  F1 Score  Precision  Recall   TP   TN  FP   FN  Time (s)
Logistic Regression   0.6022    0.7280    0.6376     0.6923  0.7280  133 5549  93 2030     23.13
      Random Forest   0.8075    0.8156    0.7802     0.8528  0.8156  725 5641   1 1438     30.12
                GBT   0.8241    0.8500    0.8337     0.8610  0.8500 1081 5553  89 1082    402.16
--- Random Forest Feature Importances ---
       Feature  Importance
   crime_index    0.973577
          Hour    0.010911
domestic_index    0.010890
      District    0.004622
Model successfully saved!
SparkSession stopped.


Yarn evidence:
 
awe used the full dataset but it wasn’t 7mil it was almost 800k


 
 
 
 
 

Task 11 full terminal:
PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> scp m2_spark_ml.py aoaltamimi@134.209.172.50:~/
aoaltamimi@134.209.172.50's password: 
m2_spark_ml.py                                                                                                                                                                                                          100% 5880    33.8KB/s   00:00    
PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> ssh aoaltamimi@134.209.172.50                  
aoaltamimi@134.209.172.50's password: 
Welcome to Ubuntu 22.04.5 LTS (GNU/Linux 5.15.0-176-generic x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/pro

 System information as of Fri May 22 18:08:49 UTC 2026

Expanded Security Maintenance for Applications is not enabled.

36 updates can be applied immediately.
4 of these updates are standard security updates.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status

New release '24.04.4 LTS' available.
Run 'do-release-upgrade' to upgrade to it.


*** System restart required ***
Last login: Fri May 22 17:54:54 2026 from 5.163.250.180
aoaltamimi@master-node:~$ spark-submit     --master yarn     --deploy-mode cluster     --driver-memory 512m     --num-executors 1     --executor-memory 1g     --executor-cores 1     --conf spark.driver.maxResultSize=128m     --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12     --conf spark.executorEnv.PYSPARK_PYTHON=python3.12     m2_spark_ml.py
26/05/22 18:09:08 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/05/22 18:09:08 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
26/05/22 18:09:10 INFO Configuration: resource-types.xml not found
26/05/22 18:09:10 INFO ResourceUtils: Unable to find 'resource-types.xml'.
26/05/22 18:09:10 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (1536 MB per container)
26/05/22 18:09:10 INFO Client: Will allocate AM container, with 896 MB memory including 384 MB overhead
26/05/22 18:09:10 INFO Client: Setting up container launch context for our AM
26/05/22 18:09:10 INFO Client: Setting up the launch environment for our AM container
26/05/22 18:09:10 INFO Client: Preparing resources for our AM container
26/05/22 18:09:10 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/commons-pool2-2.12.0.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/commons-pool2-2.12.0.jar
26/05/22 18:09:11 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/kafka-clients-3.9.0.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/kafka-clients-3.9.0.jar
26/05/22 18:09:12 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-sql-kafka-0-10_2.12-3.5.4.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/spark-sql-kafka-0-10_2.12-3.5.4.jar
26/05/22 18:09:12 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/jars/kafka/spark-token-provider-kafka-0-10_2.12-3.5.4.jar -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/spark-token-provider-kafka-0-10_2.12-3.5.4.jar
26/05/22 18:09:13 INFO Client: Uploading resource file:/home/aoaltamimi/m2_spark_ml.py -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/m2_spark_ml.py
26/05/22 18:09:13 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/python/lib/pyspark.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/pyspark.zip
26/05/22 18:09:14 INFO Client: Uploading resource file:/opt/spark-3.5.4-bin-hadoop3/python/lib/py4j-0.10.9.7-src.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/py4j-0.10.9.7-src.zip
26/05/22 18:09:15 INFO Client: Uploading resource file:/tmp/spark-5ddb0055-f2a3-41e4-bac7-165ba650f6b1/__spark_conf__6488889442909890380.zip -> hdfs://master-node:9000/user/aoaltamimi/.sparkStaging/application_1778738889964_0109/__spark_conf__.zip
26/05/22 18:09:15 INFO SecurityManager: Changing view acls to: aoaltamimi
26/05/22 18:09:15 INFO SecurityManager: Changing modify acls to: aoaltamimi
26/05/22 18:09:15 INFO SecurityManager: Changing view acls groups to: 
26/05/22 18:09:15 INFO SecurityManager: Changing modify acls groups to: 
26/05/22 18:09:15 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: aoaltamimi; groups with view permissions: EMPTY; users with modify permissions: aoaltamimi; groups with modify permissions: EMPTY
26/05/22 18:09:15 INFO Client: Submitting application application_1778738889964_0109 to ResourceManager
26/05/22 18:09:15 INFO YarnClientImpl: Submitted application application_1778738889964_0109
26/05/22 18:09:16 INFO Client: Application report for application_1778738889964_0109 (state: ACCEPTED)
26/05/22 18:09:16 INFO Client: 
         client token: N/A
         diagnostics: AM container is launched, waiting for AM container to Register with RM
         ApplicationMaster host: N/A
         ApplicationMaster RPC port: -1
         queue: root.default
         start time: 1779473355921
         final status: UNDEFINED
         tracking URL: http://master-node:8088/proxy/application_1778738889964_0109/
         user: aoaltamimi
26/05/22 18:09:38 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:09:38 INFO Client: 
         client token: N/A
         diagnostics: N/A
         ApplicationMaster host: worker-node-2
         ApplicationMaster RPC port: 39879
         queue: root.default
         start time: 1779473355921
         final status: UNDEFINED
         tracking URL: http://master-node:8088/proxy/application_1778738889964_0109/
         user: aoaltamimi
26/05/22 18:10:08 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:10:38 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:11:08 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:11:38 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:12:08 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:12:38 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:13:08 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:13:38 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:14:09 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:14:39 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:15:09 INFO Client: Application report for application_1778738889964_0109 (state: RUNNING)
26/05/22 18:15:37 INFO Client: Application report for application_1778738889964_0109 (state: FINISHED)
26/05/22 18:15:37 INFO Client: 
         client token: N/A
         diagnostics: N/A
         ApplicationMaster host: worker-node-2
         ApplicationMaster RPC port: 39879
         queue: root.default
         start time: 1779473355921
         final status: SUCCEEDED
         tracking URL: http://master-node:8088/proxy/application_1778738889964_0109/
         user: aoaltamimi
26/05/22 18:15:37 INFO ShutdownHookManager: Shutdown hook called
26/05/22 18:15:37 INFO ShutdownHookManager: Deleting directory /tmp/spark-b392868a-c86a-44a4-b35d-d8be2c5af3a7
26/05/22 18:15:37 INFO ShutdownHookManager: Deleting directory /tmp/spark-5ddb0055-f2a3-41e4-bac7-165ba650f6b1
aoaltamimi@master-node:~$ mkdir -p ~/output/spark_submit
aoaltamimi@master-node:~$ yarn logs -applicationId application_1778738889964_0109 > ~/output/spark_submit/run.log
WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR. Using value of YARN_CONF_DIR.
2026-05-22 18:16:21,367 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
aoaltamimi@master-node:~$ wc -l ~/output/spark_submit/run.log
11638 /home/aoaltamimi/output/spark_submit/run.log
aoaltamimi@master-node:~$ head -100 ~/output/spark_submit/run.log
Container: container_1778738889964_0109_01_000003 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:directory.info
LogLastModifiedTime:Fri May 22 18:15:38 +0000 2026
LogLength:7927
LogContents:
ls -l:
total 52
lrwxrwxrwx 1 hadoop hadoop   90 May 22 18:09 commons-pool2-2.12.0.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/72/commons-pool2-2.12.0.jar
-rw-r--r-- 1 hadoop hadoop   88 May 22 18:09 container_tokens
-rwx------ 1 hadoop hadoop  682 May 22 18:09 default_container_executor_session.sh
-rwx------ 1 hadoop hadoop  737 May 22 18:09 default_container_executor.sh
lrwxrwxrwx 1 hadoop hadoop   89 May 22 18:09 kafka-clients-3.9.0.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/70/kafka-clients-3.9.0.jar
-rwx------ 1 hadoop hadoop 7003 May 22 18:09 launch_container.sh
lrwxrwxrwx 1 hadoop hadoop   87 May 22 18:09 py4j-0.10.9.7-src.zip -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/76/py4j-0.10.9.7-src.zip
lrwxrwxrwx 1 hadoop hadoop   77 May 22 18:09 pyspark.zip -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/71/pyspark.zip
lrwxrwxrwx 1 hadoop hadoop   84 May 22 18:09 __spark_conf__ -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/73/__spark_conf__.zip
lrwxrwxrwx 1 hadoop hadoop  101 May 22 18:09 spark-sql-kafka-0-10_2.12-3.5.4.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/74/spark-sql-kafka-0-10_2.12-3.5.4.jar
lrwxrwxrwx 1 hadoop hadoop  112 May 22 18:09 spark-token-provider-kafka-0-10_2.12-3.5.4.jar -> /tmp/hadoop-hadoop/nm-local-dir/usercache/aoaltamimi/filecache/75/spark-token-provider-kafka-0-10_2.12-3.5.4.jar
drwx--x--- 2 hadoop hadoop 4096 May 22 18:09 tmp
find -L . -maxdepth 5 -ls:
   800691      4 drwx--x---   3 hadoop   hadoop       4096 May 22 18:09 .
   800700      4 -rw-r--r--   1 hadoop   hadoop         16 May 22 18:09 ./.default_container_executor.sh.crc
   800698      4 -rw-r--r--   1 hadoop   hadoop         16 May 22 18:09 ./.default_container_executor_session.sh.crc
   800637      4 drwx------   3 hadoop   hadoop       4096 May 22 18:09 ./__spark_conf__
   800675      4 -r-x------   1 hadoop   hadoop       1382 May 22 18:09 ./__spark_conf__/__spark_dist_cache__.properties
   800638      4 drwx------   2 hadoop   hadoop       4096 May 22 18:09 ./__spark_conf__/__hadoop_conf__
   800665     16 -r-x------   1 hadoop   hadoop      14007 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hadoop-policy.xml
   800668      4 -r-x------   1 hadoop   hadoop        620 May 22 18:09 ./__spark_conf__/__hadoop_conf__/httpfs-site.xml
   800646      4 -r-x------   1 hadoop   hadoop       3999 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hadoop-env.cmd
   800645      4 -r-x------   1 hadoop   hadoop       1501 May 22 18:09 ./__spark_conf__/__hadoop_conf__/yarn-site.xml
   800669      4 -r-x------   1 hadoop   hadoop        951 May 22 18:09 ./__spark_conf__/__hadoop_conf__/mapred-env.cmd
   800662      4 -r-x------   1 hadoop   hadoop       3414 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hadoop-user-functions.sh.example
   800648      4 -r-x------   1 hadoop   hadoop        259 May 22 18:09 ./__spark_conf__/__hadoop_conf__/core-site.xml
   800671      8 -r-x------   1 hadoop   hadoop       4113 May 22 18:09 ./__spark_conf__/__hadoop_conf__/mapred-queues.xml.template
   800667      4 -r-x------   1 hadoop   hadoop        775 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hdfs-site.xml.bak
   800659      4 -r-x------   1 hadoop   hadoop        683 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hdfs-rbf-site.xml
   800654      8 -r-x------   1 hadoop   hadoop       7095 May 22 18:09 ./__spark_conf__/__hadoop_conf__/yarn-env.sh
   800664      4 -r-x------   1 hadoop   hadoop       1351 May 22 18:09 ./__spark_conf__/__hadoop_conf__/kms-env.sh
   800650      4 -r-x------   1 hadoop   hadoop       2250 May 22 18:09 ./__spark_conf__/__hadoop_conf__/yarn-env.cmd
   800660      4 -r-x------   1 hadoop   hadoop        774 May 22 18:09 ./__spark_conf__/__hadoop_conf__/core-site.xml.bak
   800658      4 -r-x------   1 hadoop   hadoop       1764 May 22 18:09 ./__spark_conf__/__hadoop_conf__/mapred-env.sh
   800644      4 -r-x------   1 hadoop   hadoop       3321 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hadoop-metrics2.properties
   800640     20 -r-x------   1 hadoop   hadoop      16838 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hadoop-env.sh
   800647      4 -r-x------   1 hadoop   hadoop         28 May 22 18:09 ./__spark_conf__/__hadoop_conf__/workers
   800642      4 -r-x------   1 hadoop   hadoop       1149 May 22 18:09 ./__spark_conf__/__hadoop_conf__/mapred-site.xml
   800657      4 -r-x------   1 hadoop   hadoop       2567 May 22 18:09 ./__spark_conf__/__hadoop_conf__/container-executor.cfg
   800639     16 -r-x------   1 hadoop   hadoop      14451 May 22 18:09 ./__spark_conf__/__hadoop_conf__/log4j.properties
   800641      4 -r-x------   1 hadoop   hadoop       2591 May 22 18:09 ./__spark_conf__/__hadoop_conf__/yarnservice-log4j.properties
   800653      4 -r-x------   1 hadoop   hadoop       1335 May 22 18:09 ./__spark_conf__/__hadoop_conf__/configuration.xsl
   800672      4 -r-x------   1 hadoop   hadoop        682 May 22 18:09 ./__spark_conf__/__hadoop_conf__/kms-site.xml
   800643      4 -r-x------   1 hadoop   hadoop       1657 May 22 18:09 ./__spark_conf__/__hadoop_conf__/httpfs-log4j.properties
   800651     12 -r-x------   1 hadoop   hadoop       9213 May 22 18:09 ./__spark_conf__/__hadoop_conf__/capacity-scheduler.xml
   800649      4 -r-x------   1 hadoop   hadoop       1860 May 22 18:09 ./__spark_conf__/__hadoop_conf__/kms-log4j.properties
   800656      4 -r-x------   1 hadoop   hadoop        557 May 22 18:09 ./__spark_conf__/__hadoop_conf__/hdfs-site.xml
   800652      4 -r-x------   1 hadoop   hadoop       3518 May 22 18:09 ./__spark_conf__/__hadoop_conf__/kms-acls.xml
   800670      4 -r-x------   1 hadoop   hadoop       2697 May 22 18:09 ./__spark_conf__/__hadoop_conf__/ssl-server.xml.example
   800655      4 -r-x------   1 hadoop   hadoop       1190 May 22 18:09 ./__spark_conf__/__hadoop_conf__/yarn-site.xml.bak.1778738820
   800666      4 -r-x------   1 hadoop   hadoop       1484 May 22 18:09 ./__spark_conf__/__hadoop_conf__/httpfs-env.sh
   800663      4 -r-x------   1 hadoop   hadoop       2316 May 22 18:09 ./__spark_conf__/__hadoop_conf__/ssl-client.xml.example
   800661      4 -r-x------   1 hadoop   hadoop       2681 May 22 18:09 ./__spark_conf__/__hadoop_conf__/user_ec_policies.xml.template
   800673    244 -r-x------   1 hadoop   hadoop     245914 May 22 18:09 ./__spark_conf__/__spark_hadoop_conf__.xml
   800674      4 -r-x------   1 hadoop   hadoop       1357 May 22 18:09 ./__spark_conf__/__spark_conf__.properties
   800693      4 -rw-r--r--   1 hadoop   hadoop         88 May 22 18:09 ./container_tokens
   800696      4 -rw-r--r--   1 hadoop   hadoop         64 May 22 18:09 ./.launch_container.sh.crc
   800694      4 -rw-r--r--   1 hadoop   hadoop         12 May 22 18:09 ./.container_tokens.crc
   800697      4 -rwx------   1 hadoop   hadoop        682 May 22 18:09 ./default_container_executor_session.sh
   800695      8 -rwx------   1 hadoop   hadoop       7003 May 22 18:09 ./launch_container.sh
   800680     56 -r-x------   1 hadoop   hadoop      56808 May 22 18:09 ./spark-token-provider-kafka-0-10_2.12-3.5.4.jar
   800692      4 drwx--x---   2 hadoop   hadoop       4096 May 22 18:09 ./tmp
   800634    148 -r-x------   1 hadoop   hadoop     150048 May 22 18:09 ./commons-pool2-2.12.0.jar
   800627   8992 -r-x------   1 hadoop   hadoop    9204801 May 22 18:09 ./kafka-clients-3.9.0.jar
   800677    424 -r-x------   1 hadoop   hadoop     432339 May 22 18:09 ./spark-sql-kafka-0-10_2.12-3.5.4.jar
   800683     44 -r-x------   1 hadoop   hadoop      42424 May 22 18:09 ./py4j-0.10.9.7-src.zip
   800631   2380 -r-x------   1 hadoop   hadoop    2434671 May 22 18:09 ./pyspark.zip
   800699      4 -rwx------   1 hadoop   hadoop        737 May 22 18:09 ./default_container_executor.sh
broken symlinks(find -L . -maxdepth 5 -type l -ls):

End of LogType:directory.info
*******************************************************************************

Container: container_1778738889964_0109_01_000003 on worker-node-1_38887
LogAggregationType: AGGREGATED
========================================================================
LogType:launch_container.sh
LogLastModifiedTime:Fri May 22 18:15:38 +0000 2026
LogLength:7003
LogContents:
#!/bin/bash

set -o pipefail -e
export PRELAUNCH_OUT="/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0109/container_1778738889964_0109_01_000003/prelaunch.out"
exec >"${PRELAUNCH_OUT}"
export PRELAUNCH_ERR="/opt/hadoop-3.4.1/logs/userlogs/application_1778738889964_0109/container_1778738889964_0109_01_000003/prelaunch.err"
exec 2>"${PRELAUNCH_ERR}"
echo "Setting up env variables"
export JAVA_HOME=${JAVA_HOME:-"/usr/lib/jvm/java-11-openjdk-amd64"}
export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME:-"/opt/hadoop-3.4.1"}
export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME:-"/opt/hadoop-3.4.1"}
aoaltamimi@master-node:~$ exit
logout
Connection to 134.209.172.50 closed.
PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> scp aoaltamimi@134.209.172.50:~/output/spark_submit/run.log ./run.log
aoaltamimi@134.209.172.50's password: 
run.log                                                                                                                                                                                                                 100% 1146KB 748.8KB/s   00:01    
PS C:\Users\Admin\Desktop\Alfaisal University\4th Year\SE 446 milestone 2\se446-m2-group-Altamimi-AlDeri> 



Team Contribution:
Member	Tasks	Phase
Abdulaziz Al Tamimi	Tasks 1-4, 9-11	A, C
Mohamad Al Deri	Tasks 5-7	B










Note: For task 10, we made a .py for the notebook to be able to upload it to HDFS and run on cluster, another note is that we couldnt put screenshots in the readme.md file so we made a output file with screenshots of whats needed, please check it out for anything missing!
