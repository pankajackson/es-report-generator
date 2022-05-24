#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
pip3 uninstall -y -r requirements.txt || pip uninstall -y -r requirements.txt
sudo rm -rf /bin/esreportgen || sudo rm -rf /usr/local/bin/esreportgen