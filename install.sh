apt-get install python3-pip supervisor python3.7 -y
pip3 install aiohttp

cd /root/hades/hades
cp hades.conf /etc/supervisor/conf.d
supervisorctl reload
