#!/bin/bash

ProgName=$(basename $0)
LOG_PATH="${LOG_PATH:-<default>}"
LOG_LVL="${LOG_LVL:-INFO}"
LPASS_SERVICE_USER="${LPASS_SERVICE_USER:-<default>}"
ADMIN_USER="${ADMIN_USER:-<default>}"
PWGO_DB_CFG="${PWGO_DB_CFG:-<default>}"
WAIT_SECS="${WAIT_SECS:-<default>}"

export pidfile="/run/pwgo-autotag.pid"
pid=$BASHPID
echo -e "$(date): pwgo-autotag script running with pid ${pid}\n" | tee -a $LOG_PATH

if [ -f "${pidfile}" ]; then
  echo -e "$(date): already running. Exiting...\n" | tee -a $LOG_PATH
  exit 1
fi

echo "${pid}" > ${pidfile}

finish()
{
  echo -e "$(date): pwgo-autotag exiting. removing pid file...\n" | tee -a $LOG_PATH
  pkill -P ${pid}
  rm ${pidfile}

  exit
}

trap finish EXIT

cd $BASEDIR

while (( "$#" )); do
  case "$1" in
    -i|--interactive)
      INTERACTIVE=1
      shift
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1"
      exit 1
      ;;
  esac
done

if [ ! "$INTERACTIVE" ]
then
  exec 5>&1 &>>$LOG_PATH
else
  exec 5>&1
fi

LPASS_ASKPASS="<???>" lpass login $LPASS_SERVICE_USER || { echo 'lastpass login failed'; exit 1; }
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
echo "executing command $cmd"
source ./activate
eval $cmd
deactivate

if [ ! "$INTERACTIVE" ]
then
  exec 5>&-
fi

lpass logout --force
