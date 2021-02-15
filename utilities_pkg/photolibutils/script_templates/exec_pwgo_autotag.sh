#!/bin/bash

ProgName=$(basename $0)
LOG_PATH="${LOG_PATH:-<default>}"
LOG_LVL="${LOG_LVL:-INFO}"
LPASS_SERVICE_USER="${LPASS_SERVICE_USER:-<default>}"
ADMIN_USER="${ADMIN_USER:-<default>}"
PWGO_DB_CFG="${PWGO_DB_CFG:-<default>}"
WAIT_SECS="${WAIT_SECS:-<default>}"

if [ -z "$PS1" ]; then
    CMD_OUTPUT=">> $LOG_PATH"
else
    CMD_OUTPUT="| tee -a $LOG_PATH"
fi

LPASS_ASKPASS="<askpass script>" lpass login $LPASS_SERVICE_USER $CMD_OUTPUT || { echo 'lastpass login failed' $CMD_OUTPUT; exit 1; }

cmd_parts=(
    "pwgo-autotag"
    "-db"
    '<(printf "$(cat $PWGO_DB_CFG)" "$(lpass show $ADMIN_USER --password)")'
    "--wait-secs"
    "$WAIT_SECS"
    "-v"
    "$LOG_LVL"
)

raw_cmd="${cmd_parts[@]}"
export PWGO_DB_CFG
export ADMIN_USER
cmd=$(echo $raw_cmd | envsubst)

echo "executing command $cmd" $CMD_OUTPUT
eval $cmd $CMD_OUTPUT

lpass logout --force
