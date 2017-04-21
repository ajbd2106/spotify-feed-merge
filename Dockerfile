FROM gahancorpcfo/spotify-feed-merge-python 

RUN pip install nose

ADD ./sfm /sfm

WORKDIR /sfm

CMD ["python", "/sfm/test"]
