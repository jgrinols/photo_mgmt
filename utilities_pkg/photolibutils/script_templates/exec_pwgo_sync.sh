#!/bin/bash

ProgName=$(basename $0)
LOG_PATH="/var/log/pwgo-sync.log"
LOG_LVL="DEBUG"
ADMIN_USER="<username>"
LPASS_SERVICE_USER="<username>"
PWGO_USER="<piwigo admin user>"
PWGO_BASE_URL="<base url of piwigo installation>"

while (( "$#" )); do
  case "$1" in
    -u|--user)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        [ ! "$ICLOUD_USER" ] && ICLOUD_USER=$2
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

LPASS_ASKPASS="<askpass script>" lpass login $LPASS_SERVICE_USER || { echo 'lastpass login failed' >&5; exit 1; }

cat_qry="SELECT c.id FROM piwigo.categories c JOIN piwigo.categories p ON p.id = c.id_uppercat WHERE p.name = 'icloud' AND c.name = '$ICLOUD_USER'"
category_id=$(docker exec -i mariadb mysql -Nrs --user=root --password -e "$cat_qry" <<< "$(lpass show --password $ADMIN_USER)" 2>/dev/null)

cmd_parts=(
    "pwgo-sync"
    "--piwigo-base-url"
    '"$PWGO_BASE_URL"'
    "--user"
    '"$PWGO_USER"'
    "--password"
    '"<(lpass show --password $ADMIN_USER)"'
    "-v"
    '$LOG_LVL'
    "--sync-album-id"
    '$category_id'
)

if [ -n "$DRY_RUN" ]; then
  cmd_parts+=("--dry_run")
fi

cmd="${cmd_parts[@]}"

export ICLOUD_USER
export ADMIN_USER
export DRY_RUN
export PWGO_BASE_URL
export PWGO_USER
export LOG_LVL
export category_id

subst_cmd=$(echo $cmd | envsubst)
echo -e "executing:\n$subst_cmd"
bash -c "$subst_cmd"

if [ ! "$INTERACTIVE" ]
then
  exec 5>&-
fi

lpass logout --force
