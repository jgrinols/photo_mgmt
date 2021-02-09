#!/bin/bash

ProgName=$(basename $0)
LOG_PATH="/var/log/pwgo-virtualfs.log"
LOG_LVL="INFO"
LPASS_SERVICE_USER="<username>"
ADMIN_USER="<username>"
PWGO_DB_CFG="<path to json config file for piwigo db>"
PWGO_PATH="<path the piwigo galleries location>"
DEST_PATH="<path to the virtual file structure>"

if [ -z "$PS1" ]; then
    CMD_OUTPUT=">> $LOG_PATH"
else
    CMD_OUTPUT="| tee -a $LOG_PATH"
fi

LPASS_ASKPASS="<askpass script>" lpass login $LPASS_SERVICE_USER $CMD_OUTPUT || { echo 'lastpass login failed' $CMD_OUTPUT; exit 1; }

cmd_parts=(
    "pwgo-virtualfs"
    "-db"
    '<(printf "$(cat $PWGO_DB_CFG)" "$(lpass show $ADMIN_USER --password)")'
    "--piwigo-root"
    '$PWGO_PATH'
    "-v"
    '$LOG_LVL'
    "--destination-path"
    '$DEST_PATH'
    "--rebuild"
    "--monitor"
    "--remove-empty-dirs"
)

raw_cmd="${cmd_parts[@]}"
export PWGO_DB_CFG
export ADMIN_USER
export PWGO_PATH
export LOG_LVL
export DEST_PATH
cmd=$(echo $raw_cmd | envsubst)

echo "executing command $cmd" $CMD_OUTPUT
eval $cmd $CMD_OUTPUT

lpass logout --force
