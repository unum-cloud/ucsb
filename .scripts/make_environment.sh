#!/usr/bin/env bash
unset HISTFILE

mkdir -p .scripts
remote_url=http://gitlab.unum.am/devops/buildsystem/-/raw/main

# check connection
#https://stackoverflow.com/questions/929368/how-to-test-an-internet-connection-with-bash
wget -q --spider http://gitlab.unum.am

if [ $? -ne 0 ]; then
    echo "You have no connection with gitlab.unum.am: Check your connection..."
    echo "You have no connection with gitlab.unum.am: Check your connection..." >> ./.scripts/.log
    exit
fi

wget -q -O ./.scripts/prepare_env.sh $remote_url/.scripts/prepare_env.sh
bash -i .scripts/prepare_env.sh

# $1 is token, $2 is branch_name
bash -i ./.scripts/make_env.sh "$1" "$2"
