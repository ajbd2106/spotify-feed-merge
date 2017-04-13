#!/bin/bash

# Install some basics.
sudo apt-get -y update
sudo apt-get -y install wget python-dev libssl-dev libffi-dev build-essential

if [ -e "/tmp/get-pip.py" ]; 
then
  # If pip has already been downloaded it's also been installed probably.
  export PATH=/opt/python/bin:$PATH
  pip install ansible
else
  # Install pip. 
  wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
  sudo python /tmp/get-pip.py

  # And we'll need a virtualenv.
  sudo pip install virtualenv; virtualenv --system-site-packages /tmp/
  source /tmp/bin/activate; /tmp/bin/pip install ansible
fi

# Install things, now with Ansible! 
sudo mkdir -p /etc/ansible 

sudo mv -v /tmp/ansible.cfg /etc/ansible/ansible.cfg

sudo echo "export PATH=/tmp/bin:$PATH" >> /etc/profile
