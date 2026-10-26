# Spark 스트리밍 잡용 이미지. 설계: docs/adr/0002-pipeline-design.md 결정 4, 5
#
# 기본 이미지에 Kafka 소스와 ClickHouse JDBC 드라이버 jar를 미리 넣는다.
# --packages로 실행할 때마다 내려받지 않으므로 재현성이 좋고 시작이 빠르다.
# 각 jar는 Maven Central의 sha1으로 확인한 뒤 sha256을 고정했다 (2026-10-26).
# 기본 이미지에 이미 있는 의존성: scala-parallel-collections 1.2.0, jsr305, lz4/snappy/zstd

FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

ARG MAVEN=https://repo1.maven.org/maven2

# Kafka 소스 (Spark 버전과 정확히 같아야 한다) + 의존성. 버전은 spark-sql-kafka 4.1.3 pom 기준
ADD --chmod=644 --checksum=sha256:cb31ffd9c8f5527dba061db8af671d295562092e4f3f5ff42fc249fab8a8fde6 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:0535a111d7d11b2859c0a0548d5c8d172c6925f9b8d041d0f0dd38ddc5a3497c \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:5273c8bc8c0dca2211175c0d27efbd7228efae99689aac001a8e1ef8e872e9ef \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar /opt/spark/jars/

# ClickHouse JDBC 드라이버. -all = 의존성을 한 jar에 묶은(shaded) 배포본
ADD --chmod=644 --checksum=sha256:d83736e24306e11929b0cf81b3306ae903c6f51c735c509ad5680c88640d99ce \
    ${MAVEN}/com/clickhouse/clickhouse-jdbc/0.10.0/clickhouse-jdbc-0.10.0-all.jar /opt/spark/jars/

# 체크포인트 디렉터리. named volume을 처음 붙일 때 이 소유권(spark, uid 185)이 복사된다
USER root
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints
USER spark
