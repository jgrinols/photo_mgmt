#!/bin/bash

BASEDIR=$(dirname "$0")
cd $BASEDIR

LOG_PATH="${LOG_PATH:-<default>}"
LPASS_SERVICE_USER="${LPASS_SERVICE_USER:-<default>}"
ADMIN_USER="${ADMIN_USER:-<default>}"
LPASS_ICLOUD_REGEX="${LPASS_ICLOUD_REGEX:-<default>}"
DL_BASE_PATH="${DL_BASE_PATH:-<default>}"
AUTH_DB_CFG="${AUTH_DB_CFG:-<default>}"
TRACKING_DB_CFG="${TRACKING_DB_CFG:-<default>}"
LOG_LVL="${LOG_LVL:-<default>}"
AUTH_DIR="${AUTH_DIR:-<default>}"

if [ "$SSH_ORIGINAL_COMMAND" ]; then
  case "${SSH_ORIGINAL_COMMAND,,}" in
    "<valid user 1>" | "<valid user 2>")
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
    --pwgo-sync)
      PWGO_SYNC=1
      shift
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
  exec 5>&2 &>>$LOG_PATH
else
  exec 5>&2
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

LPASS_ASKPASS="${LPASS_ASKPASS:-<default>}" lpass login $LPASS_SERVICE_USER || { echo 'lastpass login failed' >&5; exit 1; }
dl_dir="${DL_BASE_PATH}/${ICLOUD_USER}"

mkdir -p "$dl_dir"
mkdir -p "$AUTH_DIR"

source "./activate"

dl_cmd_parts=(
  "icloudpd"
  "--cookie-directory"
  '"$AUTH_DIR"'
  "--directory"
  '"$dl_dir"'
  "--username"
  '"$ICLOUD_USER"'
  "--password"
  '<(lpass show --json --expand-multi --basic-regex "$LPASS_ICLOUD_REGEX" | jq -cr ".[] | select(.username == \"${ICLOUD_USER}\") | .password")'
  "--folder-structure"
  "{:%Y/%m}"
  "--convert-heic"
  "--auth-msg-config"
  '<(printf "$(cat $AUTH_DB_CFG)" "$(lpass show --password $ADMIN_USER)")'
  "--tracking-db-config"
  '<(printf "$(cat $TRACKING_DB_CFG)" "$(lpass show --password $ADMIN_USER)")'
  "-v"
  "$LOG_LVL"
)

if [ -n "$MAX_ITEMS" ]; then
  dl_cmd_parts+=("--recent")
  dl_cmd_parts+=('"$MAX_ITEMS"')
fi

if [ -n "$UNTIL_FOUND" ]; then
  dl_cmd_parts+=("--until-found")
  dl_cmd_parts+=('"$UNTIL_FOUND"')
fi

if [ -n "$DRY_RUN" ]; then
  dl_cmd_parts+=("--only-print-filenames")
  # don't output to exec_log if dry_run
  exec_log="/dev/null"
fi

dl_cmd="${dl_cmd_parts[@]}"

export auth_dir
export dl_dir
export ICLOUD_USER
export LPASS_ICLOUD_REGEX
export AUTH_DB_CFG
export ADMIN_USER
export TRACKING_DB_CFG
export MAX_ITEMS
export UNTIL_FOUND
export DRY_RUN
export INTERACTIVE

subst_dl_cmd=$(echo $dl_cmd | envsubst)

do_icloudpd() {
  (
    flock -n 9
    cmd_result=$(eval "$1") && cmd_cd=$?
    lpass logout --force
    echo "icloudpd result: $cmd_result"
    echo "icloudpd exit code: $cmd_cd"
    cnt=$(echo $cmd_result | jq '. | .downloads + .deletions')
    echo "icloudpd action count: $cnt"
    if [ $cmd_cd -eq 0 ]; then
      printf "Finished execution at %s\n" "$(date)" && date +%Y-%m-%dT%H:%M:%S >> $exec_log
      if [ $cnt -gt 0 ]; then
        sync_cmd_parts=(
          "./exec_pwgo_sync.sh"
          "--user"
          "$ICLOUD_USER"
        )
        if [ $INTERACTIVE ]; then
          sync_cmd_parts+=("-i")
        fi
        if [ $DRY_RUN ]; then
          sync_cmd_parts+=("--dry-run")
        fi
        sync_cmd="${sync_cmd_parts[@]}"
        eval $sync_cmd
      fi
    else
      printf "icloudpd execution failure\n" >&5
    fi
    set +x
  ) 9>"$BASEDIR/icloudpd.lock"
}
export -f do_icloudpd

printf "Beginning icloudpd execution at %s\n" "$(date)" >&5
#echo -e "raw_icloudpd_cmd:\n$dl_cmd"
#echo -e "icloudpd_cmd:\n$subst_dl_cmd"
#echo -e "login cmd:\n$subst_login_cmd"
invoke_cmd=$(printf "do_icloudpd '%s'" "$subst_dl_cmd")
#echo -e "invoke:\n$invoke_cmd"
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

deactivate
