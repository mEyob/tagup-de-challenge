#!/bin/bash
appHome=/home/ubuntu
sudo apt-get -y update
sudo apt-get -y install python3-venv
sudo apt-get -y install python3-pip
cd $appHome

sudo su - ubuntu -c "git clone https://github.com/mEyob/tagup-de-challenge"
python3 -m venv $appHome/example-co-env
source $appHome/example-co-env/bin/activate
pip3 install -r $appHome/tagup-de-challenge/requirements.txt