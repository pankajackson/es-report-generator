#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
pip3 install -r requirements.txt || pip install -r requirements.txt
sudo install reportgenerator.py /bin/esreportgen || sudo install reportgenerator.py /usr/local/bin/esreportgen