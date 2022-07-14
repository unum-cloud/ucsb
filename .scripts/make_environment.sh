#!/bin/bash/env bash
unset HISTFILE
set -m #&& set -e && set -u && set -o pipefail

log_file=$PWD'/.scripts/.log'
logging() {
    printf "\e[94mBuildSystem: \e[37m$1\e[0m\n" >>$log_file
}

mkdir -p .scripts
remote_url=http://gitlab.unum.am/devops/buildsystem/-/raw/main

logging "Check connection to gitlab.unum.am"
#https://stackoverflow.com/questions/929368/how-to-test-an-internet-connection-with-bash
wget -q --spider http://gitlab.unum.am

if [ $? -ne 0 ]; then
    echo "You have no connection with gitlab.unum.am: Check your connection..."
    logging "You have no connection with gitlab.unum.am: Check your connection..."
    exit 125
fi

logging "Fetching prepare_env"
wget -q -O ./.scripts/prepare_env.sh $remote_url/.scripts/prepare_env.sh
chmod +x .scripts/prepare_env.sh
logging "Running prepare_env"
.scripts/prepare_env.sh "make_environment.sh"

# $1 is token, $2 is branch_name
logging "Running make_env"
.scripts/make_env.sh "${1-}" "${2-}"
