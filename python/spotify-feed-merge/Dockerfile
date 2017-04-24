FROM gahancorpcfo/spotify-feed-merge-python 

ADD . /srv/sfm

WORKDIR /srv/sfm

RUN python setup.py build
RUN python setup.py install 

CMD ["/usr/bin/python", "/srv/sfm", "test"]
