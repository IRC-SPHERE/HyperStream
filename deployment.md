# Some notes on installing HyperStream on the new shg:

### Before Deployment: Get data out of beast's mongo:

_(on beast)_

```
sh mongo_exports.sh   
```

## All further commands on shg

### SSH keys

```
ssh-keygen  
ssh-agent /bin/bash  
ssh-add ~/.ssh/id_rsa   
ssh-add -l   
cat ~/.ssh/id_rsa.pub  
```

_Now add ssh key to bitbucket (hyperstream & sphere connector)_

### Clone Repo

```
git clone git@bitbucket.org:irc-sphere/hyperstream.git  
mv hyperstream HyperStream  
cd HyperStream  
git submodule init && git submodule update --recursive  
git config --global user.email "tom.diethe@bristol.ac.uk"  
git config --global user.name "Tom Diethe"  
git config --global push.default matching
```

### Setup virtual environment

```
bash  
sudo pip install virtualenv  
virtualenv venv  
. venv/bin/activate  
pip install -r requirements.txt
```

### Config Files

```
ln -s config_deploy.json config.json  
ln -s hyperstream_config_deploy.json hyperstream_config.json
```

### Load data into database from dumps

```
. mongo_imports.sh
```

### Run HyperStream Online Engine

```
python main.py
```

### Run HyperStream Technician Scripts

```
python scripts/display_experiments.py  
python scripts/learn_localisation_model.py 17 21  
python scripts/deploy_localisation_model.py
```

### Install Supervisor to keep the online engine running

```
sudo apt-get install supervisor  
sudo sh setup_supervisor.sh  
sudo service supervisor start
```
