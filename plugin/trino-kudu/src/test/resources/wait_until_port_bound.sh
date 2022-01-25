#!/bin/bash

set -x

wait_file() {
  local file="$1"; shift
  local wait_seconds="$1"; shift

  until test $((wait_seconds--)) -eq 0 -o -e "$file" ; do sleep 1; done

  ((++wait_seconds))
}

MAPPED_PORT_FILE="/testcontainers/mapped_port"
wait_file $MAPPED_PORT_FILE "120" # 120 second timeout

MAPPED_PORT=$(cat $MAPPED_PORT_FILE)

# Now that we know which port was mapped on the host, we can actually start up kudu

if [[ "$1" == "master" ]]; then
    export MASTER_ARGS="${MASTER_TEMPLATE_ARGS}${MAPPED_PORT}"
elif [[ "$1" == "tserver" ]]; then
    export TSERVER_ARGS="${TSERVER_TEMPLATE_ARGS}${MAPPED_PORT}"
elif [[ "$1" == "help" ]]; then
    :
fi

/kudu-entrypoint.sh "$1"
