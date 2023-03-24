#!/bin/bash 

if [[ $EUID > 0 ]]
then 
  echo "Please run with super-user privileges"
  exit 1
else
	cp ./pypeline@.service /etc/systemd/system/

	systemctl disable pypeline@
	systemctl daemon-reload
	systemctl enable pypeline@
fi
