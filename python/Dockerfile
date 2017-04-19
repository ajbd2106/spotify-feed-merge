FROM gahancorpcfo/spotify-feed-merge-python 

RUN apt-get -y install google-cloud-sdk
RUN gcloud auth activate-service-account --key-file /java/sfm.json

CMD ["/python/python/test"]
