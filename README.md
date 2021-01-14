# Big Data Streaming Demo

End-to-End example with Big Data tools. Data from a Sumo vehicle traffic simulation are sent to Kafka, processed by Flink, and real-time plotted by Matplotlib.

### Prerequesites

- Clone the [InTAS Sumo Configuration] (https://github.com/silaslobo/InTAS)
- Install [Sumo Simulator](https://sumo.dlr.de/docs/Downloads.php)
- Install [Apache Kafka](https://kafka.apache.org/quickstart)


### Setup guidelines

This procedure assumes that the InTAS (InTAS is the realistic Ingolstadt traffic scenario for SUMO), Sumo simulator, Apache Kafka, and Apache Flink
had been already installed correctly.

Once done that, to run this project please follow the following instructions:

```
git clone https://github.com/caxenie/bigdata-streaming-demo.git
cd bigdata-streaming-demo
git submodule init
git submodule update
```

Create now a virtual environment within the `bigdata-streaming-demo` folder with
Python 3.7, for making the Apache Flink APIs to work.

This is just an example of how to do it in Ubuntu if everything is already set
up:
```
python -m venv -p /usr/bin/python3.7 .venv
```

Once done, of course, install the requirements:
```
pip install -r requirements.txt
```

To make Apache Flink, it seems you should also set as default Python 3.7, so
the following variable should be set with the Python just instantiated in the
virtual environment:

```
export PYFLINK_CLIENT_EXECUTABLE="`pwd`/.venv/bin/python"
```

Then, in order to integrate Apache Kafka in the Apache Flink processing, there
is the need to add the Apache Kafka connector.
Download them:

```
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka_2.11/1.12.0/flink-connector-kafka_2.11-1.12.0.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.0/flink-sql-connector-kafka_2.11-1.12.0.jar
```

And remember to update the path in line 15-16 of the `flink_processing.py`
script to the path where you saved them.

There is also the need to run Apache Kafka. On the above link there are all the
procedures to run the server properly.

Now, open PyCharm and run the `sumo_generator.py` script (first by setting up
the `SUMO_HOME` environment variable to point to your Sumo folder).
This script will run and control the Sumo simulation, by reading at each
simulation step (each second) the amount of cars per edge. Then, it will
send messages with those information to Apache Kafka.

Within PyCharm, run also the `flink_processing.py` script that will:
- read the data from Apache Kafka
- store them in a table
- execute some query over the data
- store the result into another table
- push the stored results to another Kafka topic

Finally, we can run (better from terminal) our plotting script `plot.py`, which
reads the data from the output Kafka topic, and in real-time plots the data, by
updating the plot.
