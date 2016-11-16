. venv/bin/activate
echo_supervisord_conf > supervisord.conf  
echo "[inet_http_server]" >> supervisord.conf  
echo port=9001 >> supervisord.conf  
echo "" >> supervisord.conf
echo "[program:hyperstream]" >> supervisord.conf  
echo command=$(which python) $PWD/main.py >> supervisord.conf  
echo directory=$PWD >> supervisord.conf  
echo autostart=true >> supervisord.conf
echo autorestart=true >> supervisord.conf
echo redirect_stderr=True >> supervisord.conf
echo stderr_logfile=$PWD/logs/hyperstream.err.log >> supervisord.conf
echo stdout_logfile=$PWD/logs/hyperstream.out.log >> supervisord.conf
sudo mv supervisord.conf /etc/supervisor/
