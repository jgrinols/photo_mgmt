#!/bin/bash

ProgName=$(basename $0)
LOG_PATH="/var/log/pwgo-virtualfs.log"
LOG_LVL="INFO"
LPASS_SERVICE_USER="<username>"
ADMIN_USER="<username>"
PWGO_DB_CFG="<path to json config file for piwigo db>"
PWGO_PATH="<path the piwigo galleries location>"
DEST_PATH="<path to the virtual file structure>"

export LPASS_ASKPASS="<askpass script>"

lpass login $LPASS_SERVICE_USER || { echo 'lastpass login failed'; exit 1; }
exec 3<<<$(lpass show "$ADMIN_USER" --password)
lpass logout --force

exec 4<<<$(printf "$(cat $PWGO_DB_CFG)" "$(cat <&3)")

cmd_parts=(
    "pwgo-virtualfs"
    "-db"
    "/proc/self/fd/4"
    "--piwigo-root"
    $PWGO_PATH
    "-v"
    $LOG_LVL
    "--destination-path"
    $DEST_PATH
    "--rebuild"
    "--monitor"
    "--remove-empty-dirs"
)

cmd="${cmd_parts[@]}"
echo "executing command $cmd"
eval $cmd

exec 3>&-
exec 4>&-
