#!/bin/sh

APP_SAVE=$FLASK_APP
export FLASK_APP=ingress
python -m flask run
export FLASK_APP=$APP_SAVE
