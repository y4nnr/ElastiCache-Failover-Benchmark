# ElastiCache-Failover-Benchmark
ElastiCache Failover Benchmark tool based on Redisson for Redis cluster mode enabled

How-to:

1) Download the source code
2) Extract
3) mvn package
4) cd target
5) Use the code with the correct endpoint
java -Duri="redis://ENDPOINT:6379" -Dslots=1000,7000 -Doutperoperation=10 -Dorg.slf4j.simpleLogger.defaultLogLevel=all -jar elasticache-test-1.0-SNAPSHOT-jar-with-dependencies.jar
6) Trigger a failover with a 2 shards cluster
7) Trigger a failover with a 3 shards cluster
