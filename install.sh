apt-get install python-pip3 supervisor -y
pip3 install tornado

cd /home/hades/hades
cp hades.conf /etc/supervisor/conf.d
supervisorctl reload
