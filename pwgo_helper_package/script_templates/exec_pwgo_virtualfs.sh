#!/bin/bash

BASEDIR=$(dirname "$0")
LOG_PATH="${LOG_PATH:-<default>}"
LOG_LVL="${LOG_LVL:-INFO}"
LPASS_SERVICE_USER="${LPASS_SERVICE_USER:-<default>}"
ADMIN_USER="${ADMIN_USER:-<default>}"
PWGO_DB_CFG="${PWGO_DB_CFG:-<default>}"
PWGO_PATH="${PWGO_PATH:-<default>}"
DEST_PATH="${DEST_PATH:-<default>}"

export pidfile="/run/pwgo-virtualfs.pid"

pid=$BASHPID
echo -e "$(date): pwgo-virtualfs script running with pid ${pid}\n" | tee -a $LOG_PATH

if [ -f "${pidfile}" ]; then
  echo -e "$(date): already running. Exiting...\n" | tee -a $LOG_PATH
  exit 1
fi

echo "${pid}" > ${pidfile}

finish()
{
  echo -e "$(date): pwgo-virtualfs exiting. removing pid file...\n" | tee -a $LOG_PATH
  pkill -P ${pid}
  rm ${pidfile}

  exit
}

trap finish EXIT

cd $BASEDIR

echo -e "$(date): starting pwgo-virtualfs...\n" | tee -a $LOG_PATH
echo -e "$(date): generating pid file with id ${pid}...\n" | tee -a $LOG_PATH

login_cmd="LPASS_ASKPASS=\"<???>\" lpass login $LPASS_SERVICE_USER $CMD_OUTPUT || { echo \"lastpass login failed\" $CMD_OUTPUT; exit 1; }"
echo $login_cmd | tee -a $LOG_PATH
eval $login_cmd | tee -a $LOG_PATH

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

echo -e "executing command:\n$cmd" | tee -a $LOG_PATH
source ./activate
eval $cmd | tee -a $LOG_PATH
deactivate

lpass logout --force
