## Real Time Stock Data prediction and visualisation using Kafka-Pyspark streaming.

#### Description:
We are using kafka to publish the message, Pyspark to stream, process and analyse the incoming messages from the broker.Then using machine learning algorithms we create a predictive model to predict the test dataset generated.
Then we use flask for real time visualtion in browser.

#### Steps:

1. Kafka Producer to generate and send the data to kafka topic
2. Pyspark to stream the data from the kafka topic
3. Analyze the data, preprocess it and clean it. (Store it locally,hdfs or s3 bucket for future processing)
4. Machine learning algorithms of MLib will generate a predictive model for stock data analysis. 
5. The flask and pyspark application will generate the visualization in a web browser using the test data and predictive model.