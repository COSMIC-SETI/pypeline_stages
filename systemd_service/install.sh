#!/bin/bash 

if [[ $EUID > 0 ]]
then 
  echo "Please run with super-user privileges"
  exit 1
else
	cp ./pypeline@.service /etc/systemd/system/
	cp ./pypeline_monitor.service /etc/systemd/system/
	cp ./pypeline_monitor_service.conf /home/cosmic/conf

	systemctl disable pypeline@
	systemctl disable pypeline_monitor
	systemctl daemon-reload
	systemctl enable pypeline_monitor
	systemctl enable pypeline@
fi
