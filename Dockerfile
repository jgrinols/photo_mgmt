FROM python:3.9-buster

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends imagemagick

RUN pip3 --disable-pip-version-check --no-cache-dir install wheel
RUN pip3 --disable-pip-version-check --no-cache-dir install ./pwgo_helper_package

VOLUME /virtualfs
VOLUME /logs
CMD exec pwgo-helper agent --virtualfs-root /virtualfs 2>&1 | tee -a /logs/pwgo-helper-agent.log
