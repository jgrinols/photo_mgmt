FROM python:3.9-buster

COPY ./ /tmp/photo_mgmt/
# installing from a tarball because it resolves the symlinks to icloud_photo_downloader to real files
RUN mkdir -p /tmp/pip-tmp && tar -C /tmp/photo_mgmt/pwgo_helper_package -zcvhf /tmp/pip-tmp/pwgo_helper_package.tar.gz .
RUN pip3 --disable-pip-version-check --no-cache-dir install wheel
RUN pip3 --disable-pip-version-check --no-cache-dir install /tmp/pip-tmp/pwgo_helper_package.tar.gz

VOLUME /virtualfs
VOLUME /logs
CMD exec pwgo-helper agent --virtualfs-root /virtualfs 2>&1 | tee -a /logs/pwgo-helper-agent.log
