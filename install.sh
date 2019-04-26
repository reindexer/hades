apt-get install python3-pip supervisor python3.7 -y
python3.7 /usr/bin/pip3 install aiohttp

cd /root/hades
cp hades.conf /etc/supervisor/conf.d
supervisorctl reload
