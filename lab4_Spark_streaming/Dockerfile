ARG BASE_CONTAINER=jupyter/all-spark-notebook
FROM $BASE_CONTAINER AS base

USER root

FROM base AS builder

WORKDIR /coinbase-spark

COPY . .

RUN apt-get update && apt-get install gnupg2 -y && \
        echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
        curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
        apt-get update -y && \
        apt-get install sbt -y

RUN sbt update #prefetch dependencies & sbt

RUN sbt clean assembly && \
    echo "spark.jars ${HOME}/coinbase-spark/target/scala-2.12/coinbase-spark-assembly-0.1.0-SNAPSHOT.jar" > /usr/local/spark/conf/spark-defaults.conf

FROM base

COPY --from=builder /coinbase-spark/target/scala-2.12/coinbase-spark-assembly-0.1.0-SNAPSHOT.jar /usr/local/spark/jars
COPY Notebook.ipynb work/
