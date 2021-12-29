#!/usr/bin/env bash
unset HISTFILE

mkdir -p .scripts
remote_url=http://gitlab.unum.am/devops/buildsystem/-/raw/main

wget -q -O ./.scripts/update_env.sh $remote_url/.scripts/update_env.sh
# wget -q -O ./README.md $remote_url/README.md

bash -i ./.scripts/update_env.sh
