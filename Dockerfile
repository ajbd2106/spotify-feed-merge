FROM gahancorpcfo/spotify-feed-merge-python 

RUN pip install nose

ADD . /sfm

WORKDIR /sfm

CMD ["python", ""]
