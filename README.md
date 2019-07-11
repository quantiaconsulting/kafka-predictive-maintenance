# kafka-predictive-maintainance

## kafka-spark-notebooks
The `kafka-spark-notebooks` folder contains 4 different notebooks:

* `ks-connection-simple-err.ipynb` (scala): connects to the `errors-simple` topic and continuously save the content of the topic as parquet file * `ks-connection-simple-war.ipynb` (scala): connects to the `warnings-simple` topic and continuously save the content of the topic as parquet file 
* `data-preparation.ipynb` (scala): joins the errors and warnings streams and performs simple data preparation tasks
* `pivot.ipynb` (scala): pivots the result of the stream-stream join.

## pm-spark-notebooks
The `pm-spark-notebooks` folder contains 3 different notebooks:

* `Notebook_1_DataCleansing_FeatureEngineering.ipynb` (python): data exploration, data cleansing and feature engineering* `Notebook_2_FeatureEngineering_RollingCompute.ipynb` (python): computation of rolling features on different time periods* `Notebook_3_Labeling_FeatureSelection_Modeling.ipynb` (python): machine learning operations## schemas
The folder contains the avro schema for the warnings and errors messages.

## scripts
The folder contains the producer scripts:

* `simple-producer.py` (python): sends messages to simple kafka topics* `avro-producer.py` (python): sends messages to avro kafka topics