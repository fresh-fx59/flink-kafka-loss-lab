# Flink 1.17.0 on Java 17.
# Upstream publishes NO flink:1.17.*-java17 image (the 1.17 line ships -java8 and
# -java11 only; java17 tags start at 1.18). This image exists to match the operator's
# production combination exactly, and to make the required --add-opens list explicit.
FROM eclipse-temurin:17-jdk-jammy

ARG FLINK_VERSION=1.17.0
ARG SCALA_VERSION=2.12

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl gettext-base gosu \
 && rm -rf /var/lib/apt/lists/*

ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH

RUN set -eux; \
    curl -fsSL -o /tmp/flink.tgz \
      "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz"; \
    mkdir -p /opt; \
    tar -xzf /tmp/flink.tgz -C /opt; \
    mv "/opt/flink-${FLINK_VERSION}" "$FLINK_HOME"; \
    rm /tmp/flink.tgz; \
    groupadd -r flink && useradd -r -g flink flink; \
    chown -R flink:flink "$FLINK_HOME"

# The exact env.java.opts.all list Flink 1.18 ships as its default. The docs are
# explicit: "The list of configured arguments must not be shortened, but only
# extended." Flink 1.17 has no such default, so we add it here.
COPY java17-opts.txt /opt/java17-opts.txt
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

EXPOSE 8081 6123
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
