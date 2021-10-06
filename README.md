# Piwigo Helper

A set of command line utilities that add functionality and workflow automation to a [Piwigo](https://github.com/Piwigo/Piwigo) instance.

The utilities include:
* **Metadata Agent**: A daemon which monitors the Piwigo database for relevant changes and can perform actions such as:
  - Automatically tag photos based on subject matter and facial regonition using the [AWS Rekognition API](https://aws.amazon.com/rekognition/)
  - Sync metadata that has been modified within Piwigo back to the physical files.
  - Maintain a virtual filesystem of symbolic links structured to mirror the albums within Piwigo.
* **ICloud Downloader**: Automates downloading of new media items that have been uploaded to ICloud.
* **Piwigo Sync**: Automates the process of adding newly downloaded media items to Piwigo (typically used in conjunction with ICloud Downloader).

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

### pwgo-helper

The top-level command. Accepts parameters shared by all subcommands.

All options can be set via environment variables. The name of the environment
variable is the prefix `PWGO_HLPR_` followed by the parameter name in uppercase
and with hyphens replaced by underscores. For instance, the parameter `--log-level`
can be set with an environment variable named `PWGO_HLPR_LOG_LEVEL`.

This is also true for subcommand parameters. In this case, the environment variable name
includes the subcommand name. The `--worker-error-limit` option of the agent command
could be set by creating an environment variable `PWGO_HLPR_AGENT_WORKER_ERROR_LIMIT`.

```
pwgo-helper [OPTIONS] COMMAND [ARGS]...
```

### Options


### --env-file( <env_file>)
The path to an environment file which will be loaded before other options are resolved


### --db-conn-json( <db_conn_json>)
json string representing the database server connection parameters


### --pwgo-db-name( <pwgo_db_name>)
name of the piwigo database


### --msg-db-name( <msg_db_name>)
name of the messaging database


### --rek-db-name( <rek_db_name>)
name of the rekognition database


### --log-level( <log_level>)
specifies the verbosity of the log output


* **Options**

    CRITICAL | ERROR | WARNING | INFO | DEBUG | NOTSET



### --lib-log-level( <lib_log_level>)
specifies the verbosity of logging from standard library and third party modules


* **Options**

    CRITICAL | ERROR | WARNING | INFO | DEBUG | NOTSET



### --dry-run(, --no-dry-run()
don’t actually do anything–just pretend

#### agent

Command used to auto generate related tags when a new tag is inserted in database

```
pwgo-helper agent [OPTIONS]
```

### Options


### --piwigo-galleries-host-path( <piwigo_galleries_host_path>)
**Required** Host path of the piwigo gallieries folder. Can be any path that can be opened
as a virtual filesystem by the fs package


### --rek-access-key( <rek_access_key>)
**Required** rekognition aws access key


### --rek-secret-access-key( <rek_secret_access_key>)
**Required** rekognition aws secret access key


### --rek-region( <rek_region>)
**Required** rekognition aws region


### --rek-collection-arn( <rek_collection_arn>)
**Required** rekognition aws arn for default collection


### --rek-collection-id( <rek_collection_id>)
**Required** rekognition collection id


### --image-crop-save-path( <image_crop_save_path>)
Indicates the directory to which to save crops of faces detected in images. Crops are not saved by default.


### --virtualfs-root( <virtualfs_root>)
path to the root of the album-based virtual filesystem


### --virtualfs-allow-broken-links()
create symlinks even when source file path is not found


### --virtualfs-remove-empty-dirs()
remove directories in virtual fs root that become empty when a symlink is removed


### --virtualfs-category-id( <virtualfs_category_id>)
the root category for the virtual fs. subcategories of the specified category will be included


### --workers( <workers>)
Number of workers to handle event queue


### --worker-error-limit( <worker_error_limit>)
Number of processing errors to allow before quitting


### -d(, --debug()
Enable debug mode for asyncio event loop


### --initialize-db()
Run database initialization scripts at startup

#### icdownload

```
pwgo-helper icdownload [OPTIONS]
```

### Options


### -d(, --directory( <directory>)
**Required** Local directory that should be used for download


### -u(, --username( <username>)
**Required** icloud username


### -p(, --password( <password>)
**Required** icloud password


### --cookie-directory( <cookie_directory>)
Directory to store cookies for authentication (default: ~/.pyicloud)


### --size( <size>)
Image size to download (default: original)


* **Options**

    original | medium | thumb



### --recent( <recent>)
Number of recent photos to download (default: download all photos)


### --until-found( <until_found>)
Download most recently added photos until we find x number of previously downloaded consecutive photos (default: download all photos)


### -a(, --album( <album>)
Album to download (default: All Photos)


### -l(, --list-albums()
Lists the available albums


### --skip-videos()
Don’t download any videos (default: Download all photos and videos)


### --force-size()
Only download the requested size (default: download original if size is not available)


### --convert-heic()
Auto-convert heic images to jpeg (default: retain heic format)


### --convert-mov()
Auto-convert mov (quicktime) files to mp4 (default: retain mov format)


### --auto-delete()
Scans the “Recently Deleted” folder and deletes any files found in there. (If you restore the photo in iCloud, it will be downloaded again.)


### --only-print-filenames()
Only prints the filenames of all files that will be downloaded (not including files that are already downloaded.)(Does not download or delete any files.)


### --folder-structure( <folder_structure>)
Folder structure (default: {:%Y/%m}). If set to ‘none’ all photos will just be placed into the download directory


### --auth-msg-db( <auth_msg_db>)
**Required** name of database that contains auth message table


### --auth-msg-tbl( <auth_msg_tbl>)
**Required** name of table that receives auth message


### --mfa-timeout( <mfa_timeout>)
number of seconds to wait for mfa code before raising error


### --auth-phone-digits( <auth_phone_digits>)
**Required** the last two digits of the phone number that will forward the auth code


### --tracking-db( <tracking_db>)
name of the database used for tracking downloads


### --lookback-days( <lookback_days>)
#### sync

```
pwgo-helper sync [OPTIONS]
```

### Options


### -b(, --base-url( <base_url>)
**Required** url specifiying the web root of the Piwigo installation


### -u(, --user( <user>)
**Required** Admin user to use to login and execute synchronization


### -p(, --password( <password>)
**Required** admin password


### --sync-album-id( <sync_album_id>)
Id of an album to synchronize (default: sync all)


### --skip-metadata()
Don’t sync file metadata to Piwigo


### --directories-only()
should the sync be limited to only adding new directory structure


### --add-to-caddie()
should synced files be added to the caddie


### --add-missing-md5()
should md5 hashes be computed for any photos where they’re missing


### --file-access-level( <file_access_level>)
**Required** who should be able to see the imported media


* **Options**

    All | Contacts | Friends | Family | Admins



### --md5-block-size( <md5_block_size>)
**Required** number of hashes to compute per request

#### version

outputs the pwgo-helper version

```
pwgo-helper version [OPTIONS]
```
