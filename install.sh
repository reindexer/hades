apt-get install python3-pip supervisor -y
pip3 install aiohttp

cd /root/hades/hades
cp hades.conf /etc/supervisor/conf.d
supervisorctl reload
