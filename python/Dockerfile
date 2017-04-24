FROM gahancorpcfo/spotify-feed-merge-python 

RUN pip install nose

ADD . /srv/sfm

WORKDIR /srv/sfm

RUN nosetests

CMD ["/usr/bin/python", "/srv/sfm/sfm.py"]
