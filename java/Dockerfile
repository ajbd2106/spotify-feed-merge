FROM ubuntu:16.04

RUN apt-get -y update; apt-get -y install ansible git gradle sudo; ansible -m lineinfile -a 'line="localhost ansible_connection=local" create=yes dest=/etc/ansible/hosts state=present' localhost

RUN git clone https://github.com/alexander-laughlin/java; ansible-playbook java/java.yml 

WORKDIR /java/java

RUN gradle
