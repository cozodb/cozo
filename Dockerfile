FROM ghcr.io/cross-rs/aarch64-unknown-linux-gnu:main
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN apt-get update && \
    apt-get install --assume-yes openjdk-11-jdk-headless
