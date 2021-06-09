FROM python:3.9-alpine

COPY ./ /tmp/photo_mgmt/
# installing from a tarball because it resolves the symlinks to icloud_photo_downloader to real files
RUN mkdir -p /tmp/pip-tmp && tar -C /tmp/photo_mgmt/utilities_pkg -zcvhf /tmp/pip-tmp/utilities_pkg.tar.gz .
RUN pip3 --disable-pip-version-check --no-cache-dir install /tmp/pip-tmp/utilities_pkg.tar.gz
