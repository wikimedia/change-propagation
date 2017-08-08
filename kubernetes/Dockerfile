FROM wikimedia/nodejs
RUN apt-get update && apt-get install -y libsasl2-dev librdkafka-dev && rm -rf /var/lib/apt/lists/*
RUN mkdir /opt/service
ADD . /opt/service
WORKDIR /opt/service
ENV HOME=/root LINK=g++ BUILD_LIBRDKAFKA="0" IN_DOCKER=1 UV_THREADPOOL_SIZE=128
RUN rm -rf ./node_modules && npm install && npm dedupe
COPY ./kubernetes/config.yaml /etc/config.yaml
CMD node /opt/service/server.js -c /etc/config.yaml