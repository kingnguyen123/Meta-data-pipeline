# Realtime-Algorithmic-Trading
## Introduction

>This project focuses on end-to-end data engineering project focused on build a data pipepline can handle a large amount of data.

## Architecture
The architecture of the project is as follows:
![Architecture diagramming](https://github.com/kingnguyen123/Meta-data-pipeline/blob/main/Blank%20diagram%20(2).png)

## Technology Used

- **Programming Language**: Python
- **Message streaming and data ingestion**: Apache Kafka and Redpanda
-**Data streamming**: Apache spark
- **Containerized deployment of the architecture**: Docker
## Conclusion

The main keys of this architecture are Kafka and Spark. Instead of using Zookeeper, Kafka deployed in KRaft mode for simpler management and better performance.
Kafka will handle incoming data from producers, organizing it into topics and partitions to scale and manage. Then, Spark Streaming jumps into real-time streaming process.

Thank you for exploring this project. If you have any questions or suggestions, please feel free to reach out or contribute to the repository.
