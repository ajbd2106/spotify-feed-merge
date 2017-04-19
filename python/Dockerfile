FROM ubuntu:16.04

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install ansible; ansible -m get_url -a "url=https://bootstrap.pypa.io/get-pip.py dest=/tmp/get-pip.py" localhost 
RUN ansible -m lineinfile -a 'line="localhost ansible_connection=local" dest=/etc/ansible/hosts create=yes state=present' localhost
RUN python /tmp/get-pip.py
ADD ./ /python
RUN ansible-playbook /python/build/python.yml
RUN ansible-playbook /python/build/google.yml
RUN gcloud auth activate-service-account --key-file /python/sfm.json 
