FROM gahancorpcfo/spotify-feed-merge-python 

RUN pip install nose

ADD ./ /code

WORKDIR /code

CMD ["python", "/code/test"]
