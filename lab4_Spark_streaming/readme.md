# CoinBase Websocket source for Spark Structured Streaming

## Overview

This project builds a Docker image with Pyspark and a package that implements a Websocket source for Spark Structure Streaming configured to work with the [CoinBase Websocket API](https://docs.cloud.coinbase.com/exchange/docs/overview)
which is a stream of Cryptocurrency trading transactions. The CoinBase streams are available via several [channels](https://docs.cloud.coinbase.com/exchange/docs/channels).

You can use the following channels:
- heartbeat
- ticker
- status
- level2
- auction

To subscribe to a specific channel one has to pass `schema` option value:
`spark.readStream.format("ws").option("schema", "ticker").load()`

## Build and Run

- Build Docker image (optional, the image will be pulled from DockerHub)
```bash
make image
```

- Run Docker container
```bash
make run
```

Open Jupyter in the browser: http://127.0.0.1:8888. Note that the current working directory where `make run` was executed will be mounted as a working directory for Jupyter. 


## Assignments
Open `Notebook.ipynb` in Jupyter to see the assignmets. 


## Running spark structured streaming queries

Please keep in mind that you need to manually stop the session using `spark.stop()`.
Even though some exceptions may occur this closes the websocket session correctly.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, unix_timestamp, window

spark = SparkSession.builder
        .config("spark.sql.streaming.schemaInference", True).getOrCreate()

stream = spark.readStream\
              .format("ws")\
              .option("schema", "ticker").load()

query = stream.select("side", "product_id", "last_size",\ 
                      "best_bid", "best_ask", "time")\
              .writeStream\
              .format("console")\
              .outputMode("append")\
              .option("truncate", "false").start()

query.awaitTermination(10)
query.stop()
spark.stop()
```

If one starts a new query and accidentally loses the reference to it before stopping (e.g. python interpreter will fail on some instruction before stopping), he/she should kill spark session by explicitly calling `spark.stop()` in a new cell. Otherwise, the websockets will still be alive and will stream the data.


