#!/bin/bash

BASEDIR=$(dirname "$0")
LOG_PATH="/var/log/icloudpd.log"
LPASS_SERVICE_USER="<username>"
ADMIN_USER="<username>"
LPASS_ICLOUD_REGEX="<regex for locating icloud acct entries in lpass>"
ICLOUDPD_VENV_PATH="<location of icloudpd virt env>"
DL_BASE_PATH="<download location>"
AUTH_DB_CFG="<path to json config file for auth msg db>"
TRACKING_DB_CFG="<path to json config file for tracking db>"

if [ "$SSH_ORIGINAL_COMMAND" ]; then
  case "${SSH_ORIGINAL_COMMAND,,}" in
    "<valid_user_1>" | "<valid_user_2>")
      ICLOUD_USER=${SSH_ORIGINAL_COMMAND,,}
      ;;
    *)
      echo "Error: invalid icloud user"
      exit 1
      ;;
  esac
fi

while (( "$#" )); do
  case "$1" in
    -u|--user)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        [ ! "$ICLOUD_USER" ] && ICLOUD_USER=$2
        shift 2
      fi
      ;;
    -r|--recent)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        MAX_ITEMS=$2
        shift 2
      fi
      ;;
    --until-found)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        UNTIL_FOUND=$2
        shift 2
      fi
      ;;
    -d|--dry-run)
      DRY_RUN=1
      shift
      ;;
    -i|--interactive)
      INTERACTIVE=1
      shift
      ;;
    -f|--force)
      FORCE_RUN=1
      shift
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1"
      exit 1
      ;;
  esac
done

if [ "$SSH_ORIGINAL_COMMAND" ] && [ ! "$INTERACTIVE" ]
then
  exec 5>&1 &>>$LOG_PATH
else
  exec 5>&1
fi

exec_log="$BASEDIR/exec_log[$ICLOUD_USER]"
echo $exec_log
# user exec log exists
if [ -f "$exec_log" ] && [ -z "$FORCE_RUN" ]
then
  last_entry=$(tail -n 1 "$exec_log")
  # the log has an entry
  if [ ! -z "$last_entry" ]
  then
    last_run=$(date --date="$last_entry" +%s)
    # the last entry can be parsed as a date
    if [ ! -z "$last_run" ]
    then
      cutoff_tm=$(date -d 'now - 30 minutes' +%s)
      # the last exec date is less than 30 mins ago
      if (( cutoff_tm < last_run ))
      then
        printf "icloudpd was run less than 30 mins ago for user $ICLOUD_USER...not running\n" >&5
        exit 0
      fi
    fi
  fi
fi

cd $ICLOUDPD_VENV_PATH

LPASS_ASKPASS="<askpass script>" lpass login $LPASS_SERVICE_USER || { echo 'lastpass login failed' >&5; exit 1; }
dl_dir="${DL_BASE_PATH}/${ICLOUD_USER}"

mkdir -p "$dl_dir"

source "./bin/activate"

exec 3<<<"$user_pw"
exec 4<<<$(printf "$(cat $AUTH_DB_CFG)" '<(lpass show --password "$ADMIN_USER")')
exec 6<<<$(printf "$(cat $TRACKING_DB_CFG)" "$admin_pw")

dl_cmd_parts=(
  "icloudpd"
  "--cookie-directory"
  "\"./.auth\""
  "--directory"
  "\"$dl_dir\""
  "--username"
  "$ICLOUD_USER"
  "--password"
  '<(lpass show --json --expand-multi --basic-regex "$LPASS_ICLOUD_REGEX" | jq -cr ".[] | select(.username == \"${ICLOUD_USER}\") | .password")'
  "--skip-live-photos"
  "--folder-structure"
  "{:%Y/%m}"
  "--convert-heic"
  "--auth-msg-config"
  '<(printf "$(cat $AUTH_DB_CFG)" "<(lpass show --password \"$ADMIN_USER\")")'
  "--tracking-db-config"
  '<(printf "$(cat $TRACKING_DB_CFG)" "<(lpass show --password \"$ADMIN_USER\")")'
)

if [ -n "$MAX_ITEMS" ]; then
  dl_cmd_parts+=("--recent")
  dl_cmd_parts+=("$MAX_ITEMS")
fi

if [ -n "$UNTIL_FOUND" ]; then
  dl_cmd_parts+=("--until-found")
  dl_cmd_parts+=("$UNTIL_FOUND")
fi

if [ -n "$DRY_RUN" ]; then
  dl_cmd_parts+=("--only-print-filenames")
  # don't output to exec_log if dry_run
  exec_log="/dev/null"
fi

dl_cmd="${dl_cmd_parts[@]}"

do_icloudpd() {
  (
    flock -n 9
    eval "$1" && printf "Finished execution at %s\n" "$(date)" && date +%Y-%m-%dT%H:%M:%S >> $exec_log || printf "icloudpd execution failure\n"
    date +%y
  ) 9>"$BASEDIR/icloudpd.lock"
}
export -f do_icloudpd

printf "Beginning icloudpd execution at %s\n" "$(date)" >&5
invoke_cmd=$(printf "do_icloudpd '%s'" "$dl_cmd")
echo -e "invoke:\n$invoke_cmd"
export invoke_cmd
export exec_log

if [ "$SSH_ORIGINAL_COMMAND" ] && [ ! "$INTERACTIVE" ]
then
  exec 5>&-
fi

# run in the background if invoking via ssh--interactive otherwise
if [ "$SSH_ORIGINAL_COMMAND" ] && [ ! "$INTERACTIVE" ]
then
  nohup bash -c "$invoke_cmd" &
else
  bash -c "$invoke_cmd"
fi

lpass logout --force

deactivate
