# TFM-Master-Kschool Francisco Lopez
Evaluation exercise for Big Data Architech 
K-Means machine Learning over Kafka's stream. Using Apache Spark and Lightning Graph(on Heroku) on visualization server.

## Building
Everything is built on a Centos 6
This project is developed using maven and scala.


## Dependencies

### [Cloudera](http://www.cloudera.com) - cdh5.11.0 

**cloudera** s open-source Apache Hadoop distribution, CDH (Cloudera Distribution Including Apache Hadoop), targets enterprise-class deployments of that technology. Cloudera says that more than 50% of its engineering output is donated upstream to the various Apache-licensed open source projects (Apache Hive, Apache Avro, Apache HBase, and so on) that combine to form the Hadoop platform.

### <a name="lightning"></a>[Lightning Graph Server](http://lightning-viz.org/) [![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/lightning-viz/lightning/tree/master)

Lightning is a data-visualization server providing API-based access to reproducible, web-based, interactive visualizations.This version is SBT compilated on Scala 2.11.

### [Maven](https://maven.apache.org) - 3.0.4

**maven** is an open source build tool for Scala and Java projects, similar to Ant or SBT.

### [Apache Spark](http://spark.apache.org) - 2.1.0

Apache Spark is an open-source cluster computing framework originally developed in the AMPLab at UC Berkeley. In contrast to Hadoop's two-stage disk-based MapReduce paradigm, Spark's in-memory primitives provide performance up to 100 times faster for certain applications. By allowing user programs to load data into a cluster's memory and query it repeatedly, Spark is well suited to machine learning algorithms.

### [Apache Haddop](http://hadoop.apache.org) - 2.6.0

Apache Hadoop is an open-source software framework written in Java for distributed storage and distributed processing of very large data sets on computer clusters built from commodity hardware. All the modules in Hadoop are designed with a fundamental assumption that hardware failures are commonplace and thus should be automatically handled in software by the framework.

### [Scala](http://scala-lang.org) - 2.11.8

Scala is an object-functional programming language for general software applications. Scala has full support for functional programming and a very strong static type system. This allows programs written in Scala to be very concise and thus smaller in size than other general-purpose programming languages. Many of Scala's design decisions were inspired by criticism of the shortcomings of Java

### [Kafka](http://kafka.apache.org) - 0.10.0.1

Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. Its storage layer is essentially a "massively scalable pub/sub message queue architected as a distributed transaction log," making it highly valuable for enterprise infrastructures to process streaming data. Additionally, Kafka connects to external systems (for data import/export) via Kafka Connect and provides Kafka Streams, a Java stream processing library.

### [Kudu](https://kudu.apache.org/) - 1.3.0

Apache Kudu is a free and open source column-oriented data store of the Apache Hadoop ecosystem. It is compatible with most of the data processing frameworks in the Hadoop environment. It provides a completes Hadoop's storage layer to enable fast analytics on fast data.

### [Flume](https://flume.apache.org/) - 1.7.0

Apache Flume is a distributed, reliable, and available service for efficiently collecting, aggregating, and moving large amounts of log data. It has a simple and flexible architecture based on streaming data flows. It is robust and fault tolerant with tunable reliability mechanisms and many failover and recovery mechanisms. It uses a simple extensible data model that allows for online analytic application.


### [Java ](https://www.java.com/) - Cloudera Installed Edition - 1.8.0_60 

A general-purpose computer programming language designed to produce programs that will run on any computer system.
