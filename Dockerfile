FROM ghcr.io/kameshsampath/snow-dev:py-311

USER me

WORKDIR $HOME

ADD --chown=me:me src/ /home/me/app/

ADD --chown=me:me requirements.txt /home/me/app/requirements.txt
ADD --chown=me:me scripts/bin/run.app /home/me/.local/bin/run
ADD --chown=me:me app.py /home/me/.local/bin/slack-bot
ADD --chown=me:me scripts/bin/wait_for_config /home/me/.local/bin/wait_for_config

RUN pip install --no-cache --user -r /home/me/app/requirements.txt
