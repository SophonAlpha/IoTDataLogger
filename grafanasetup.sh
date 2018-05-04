#!/bin/bash

# Create InfluxDB and Grafana 4.4.3 (latest)
source /etc/os-release
OPSYS=${ID^^}
curl -sL https://repos.influxdata.com/influxdb.key | sudo apt-key add -
test $VERSION_ID = "8" && echo "deb https://repos.influxdata.com/debian jessie stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
test $VERSION_ID = "9" && echo "deb https://repos.influxdata.com/debian jessie stable" | sudo tee /etc/apt/sources.list.d/influxdb.list
curl https://bintray.com/user/downloadSubjectPublicKey?username=bintray | sudo apt-key add -
[[ $OPSYS == *"BIAN"* ]] && [[ $(uname -m) == *"armv6"* ]] && echo "deb https://dl.bintray.com/fg2it/deb-rpi-1b jessie main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
[[ $OPSYS == *"BIAN"* ]] && [[ $(uname -m) == *"armv7l"* ]] && echo "deb https://dl.bintray.com/fg2it/deb jessie main" | sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get -y $AQUIET remove --purge grafana grafana-data
sudo apt-get -y $AQUIET autoremove
sudo apt-get -y update && sudo apt-get install -y apt-transport-https curl influxdb grafana
sudo systemctl daemon-reload
sudo systemctl enable influxdb
sudo systemctl start influxdb
sudo systemctl enable grafana-server
sudo systemctl start grafana-server

