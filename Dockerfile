# Dockerfile for foundational Linux-based testing

FROM ubuntu:20.04

# Avoid interactive prompts during package installation
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=America/New_York

RUN apt-get update && \
    apt-get install -y tzdata \
    # Configure tzdata (this makes sure tzdata sees the $TZ environment variable)
    && ln -fs /usr/share/zoneinfo/$TZ /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata

# Continue with other steps as needed
...

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    zip \
    software-properties-common \
    git \
    && apt-get clean

RUN curl -s "https://get.sdkman.io" | bash
ENV SDKMAN_DIR="/root/.sdkman"
ENV JAVA_HOME="$SDKMAN_DIR/candidates/java/current"

RUN bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && \
             sdk install java 11.0.11-open && \
             sdk use java 11.0.11-open && \
             java -version"

WORKDIR /app
COPY scripts /app/scripts

CMD ["bash", "/app/scripts/run-tests.sh"]