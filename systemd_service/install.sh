#!/bin/bash 

if [[ $EUID > 0 ]]
then 
  echo "Please run with super-user privileges"
  exit 1
else
	cp ./pypeline@.service /etc/systemd/system/
	cp ./pypeline_service.conf /mnt/slow/operations/share/

	systemctl disable pypeline@
	systemctl daemon-reload
	systemctl enable pypeline@
fi
