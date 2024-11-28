FROM ghcr.io/kameshsampath/snow-dev:py-311

USER me

WORKDIR /home/me/app

ADD . /home/me/app

RUN pip install --no-cache --user -r requirements.txt

CMD ["python","app.py"]