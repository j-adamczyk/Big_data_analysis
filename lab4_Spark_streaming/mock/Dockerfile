FROM alpine:3.14.2

RUN \
  wget -q -O scala-cli.gz https://github.com/VirtusLab/scala-cli/releases/download/v0.0.7/scala-cli-x86_64-pc-linux-static.gz && gunzip scala-cli.gz && \
  chmod +x scala-cli && \
  mv scala-cli /usr/bin/

COPY Mock.scala /

RUN \
  echo "println(1)" | scala-cli -S 3.0.2 - && \
  echo "// using lib org.java-websocket:Java-WebSocket:1.5.2" | scala-cli -S 3.0.2  - && \
  echo "// using lib org.slf4j:slf4j-simple:1.7.25" | scala-cli -S 3.0.2  -

ENTRYPOINT [ "scala-cli" ]