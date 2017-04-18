FROM ubuntu:16.04

RUN apt-get -y update; apt-get -y upgrade; apt-get -y install ansible; ansible -m get_url -a "url=https://bootstrap.pypa.io/get-pip.py dest=/tmp/get-pip.py" localhost 
RUN ansible -m lineinfile -a 'line="localhost ansible_connection=local" dest=/etc/ansible/hosts create=yes state=present' localhost
RUN python /tmp/get-pip.py
ADD . /src
RUN ls -lah /src
RUN ansible-playbook /src/build/python.yml
WORKDIR /src/python
RUN ./test
