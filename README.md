# Piwigo Helper

A set of command line utilities that add functionality and workflow automation to a [Piwigo](https://github.com/Piwigo/Piwigo) instance.

The utilities include:
- **Metadata Agent**: A daemon which monitors the Piwigo database for relevant changes and can perform action such as:
\* Automatically tag photos based on subject matter and facial regonition using the AWS [Rekognition](https://aws.amazon.com/rekognition) API
\* Sync metadata that has been modified within Piwigo back the the physical files.
\* Maintain a virtual filesystem of symbolic links structured to mirror the albums within Piwigo.
- **ICloud Downloader**: Automates downloading of new media items that have been uploaded to ICloud.
- **Piwigo Sync**: Automates the process of adding newly downloaded media items to Piwigo (typically used in conjunction with ICloud Downloader).

## Install

The tools are packaged into a docker container which can be downloaded via `docker pull ghcr.io/jgrinols/photo_mgmt:latest` or used within docker-compose.

```
piwigo-helper:
  image: ghcr.io/jgrinols/photo_mgmt:latest
  volumes:
    - </host/photos/location>:</host/photos/location>
    - </host/photos/share/location>:/virtualfs
  depends_on:
    - piwigo
    - mariadb
```

## Configuration

Together the utilities are highly configurable and thus provide a host of configuration options that can be passed as command line options or via environment variables.
Typically the best way to manage configuration is by using docker-compose and an `.env` file.

```
.env

PWGO_HLPR_LOG_LEVEL=INFO
PWGO_DRY_RUN=0
PWGO_HLPR_DB_CONN_JSON={"host": "localhost", "port": "3306", "user": "db_user", "password": "db_password"}
PWGO_HLPR_AGENT_WORKERS=25
PWGO_HLPR_ICDOWNLOAD_COOKIE_DIRECTORY=/auth/icloud
PWGO_HLPR_SYNC_BASE_URL=http://piwigo
...
```

```
docker-compose.yml

piwigo-helper:
  image: ghcr.io/jgrinols/photo_mgmt:latest
  volumes:
    - </host/photos/location>:</host/photos/location>
    - </host/photos/share/location>:/virtualfs
  environment:
    - PWGO_HLPR_LOG_LEVEL
    - PWGO_DRY_RUN
    - PWGO_HLPR_DB_CONN_JSON
    - PWGO_HLPR_AGENT_WORKERS
    - PWGO_HLPR_ICDOWNLOAD_COOKIE_DIRECTORY
    - PWGO_HLPR_SYNC_BASE_URL
    ...
  depends_on:
    - piwigo
    - mariadb
```

## Command Reference
