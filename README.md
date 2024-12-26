# Comparison of Execution Time, Throughput, and Memory Utilization for Hadoop and Spark

This repository contains implementations of queries using Hadoop MapReduce and Apache Spark. The primary purpose is to compare **execution time**, **throughput**, and **memory utilization** between Hadoop and Spark for similar operations on large datasets.

## Folder Structure

### HadoopQueries
Contains Java-based implementations of Hadoop MapReduce jobs:
- **AvgOrderQtyByCategory.java**: Calculates the average order quantity grouped by product category.
- **TopProductsBySales.java**: Identifies the top products by total sales.
- **WordCount.java**: A classic word count program for text analysis.

### SparkQueries
Contains Scala-based implementations of Spark jobs:
- **AvgOrderQtyByCategory.scala**: Computes the average order quantity grouped by product category using Spark.
- **TopProductsBySales.scala**: Identifies the top products by sales in Spark.
- **WordCount.scala**: A word count program implemented in Spark.

## Objective

The goal of this project is to compare the following performance metrics:
1. **Execution Time**: Time taken to complete the query.
2. **Throughput**: Number of records processed per second.
3. **Memory Utilization**:
   - Heap Memory Utilized (MB)
   - Non-Heap Memory Utilized (MB)

## Prerequisites

Ensure the following are installed and configured:
- Hadoop
- Spark
- Java Development Kit (JDK)
- Scala
- Git

