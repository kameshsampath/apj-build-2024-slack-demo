# Cortex Analyst with Slack

A simple demo showcasing the integration of [Snowflake Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst) with Slack.

The demo also covers ChatOps aspects using the [Snowflake Python API](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/reference/latest/index) for setting up Snowflake resources such as generating [RSA Keypair](https://docs.snowflake.com/en/user-guide/key-pair-auth#configuring-key-pair-authentication), setting user public keys, creating config, databases, tables, stages, upload semantic models, etc. All containerized for an effective DevOps.

## Prerequisites

- Snowflake Account (get a free trial from https://signup.snowflake.com if needed)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) to setup your Snowflake connection locally. This will be used by the Slack-bot application when connecting to Snowflake.
- Snowflake [KeyPair Authentication](https://docs.snowflake.com/user-guide/key-pair-auth#configuring-key-pair-authentication) setup, as REST API will use JWT for secure communication
- A Slack Workspace where you can install this Slack-bot app. [Learn how to create a Slack workspace](https://slack.com/intl/en-in/help/articles/206845317-Create-a-Slack-workspace)
- Slack tokens setup - Follow the [Bolt Python getting started guide](https://tools.slack.dev/bolt-python/getting-started#tokens-and-installing-apps)

> [!NOTE]  
> Check out [direnv](https://direnv.net/) for a neat and clean way to manage your environment variables.


## Setup Environment 

### Snowflake Config Environment

The following variables are used to generate RSA KeyPair and set th Snowflake user `SNOWFLAKE_USER` public key and finally generate the Snowflake `config.toml` to be used connecting with Snowflake.

The `config` docker compose service will use `.env`

```shell
# application log level
APP_LOG_LEVEL=DEBUG
# Snowflake Account to use 
SNOWFLAKE_ACCOUNT=your snowflake account id
# Snowflake User
SNOWFLAKE_USER=your snowflake user name
# Snowflake User Password
SNOWFLAKE_PASSWORD=your snowflake user password
# Snowflake Role - should be able to create DB, schema and objects under them, alter users, 
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### Slack Bot Environment

```shell
cp $DEMO_HOME/.env.example $DEMO/.env
```

The `slack-bot` docker compose service will use `.env.bot`:

```shell
# application log level
APP_LOG_LEVEL=DEBUG
# Snowflake connection name to use
SNOWFLAKE_DEFAULT_CONNECTION_NAME="default"
# Slack API Bot Token
SLACK_BOT_TOKEN=your slack bot token
# Slack API App Token
SLACK_APP_TOKEN=your slack app token
# The private key path (defaulted to the one within container)
PRIVATE_KEY_FILE_PATH=/home/me/.snowflake/snowflake_user.p8
```

```shell
cp $DEMO_HOME/.env.bot.example $DEMO/.env.bot
```

> [!IMPORTANT]
> Update the `$DEMO/.env` and `$DEMO/.env.bot` to match your settings.

## Setup(Easy Way)

With your environment files ready fire up docker

```
cd $DEMO_HOME
docker-compose up -d
```

### Docker Compose Service::`config`

- Config will setup Snowflake RSA KeyPair 
- Creates `/home/me/.snowflake/config.toml` to use RSA KeyPair

### Docker Compose Service::`slack-bot`

- Will start the slack-bot app 
- Uses the Snowflake Connection from `/home/me/.snowflake/config.toml`

### Slack Commands

- `setup`
- `cortalyst` (Cortex Analyst :D)
- `cleanup`

## App Demo

[![Demo Video](https://img.youtube.com/vi/IwLrV_hJtuE/0.jpg)](https://www.youtube.com/watch?v=IwLrV_hJtuE)

## Setup(Hard Way)

> [!IMPORTANT]
- The `.env` and `.env.bot` needs to be sourced before you start the bot
- Setup RSA KeyPair Auth with Snowflake for your account, simple trick

```shell
# not use daemon mode so that you can watch the logs, kill the container after that
docker compose up config
# once you see it successful settings check for logs for message ` User keys configured and working`
./scripts/bin/docker-copy.sh
```

`./scripts/bin/docker-copy.sh` will copy the `/home/me/.snowflake` to `$PWD/.snowflake`. Set `SNOWFLAKE_HOME` to and all set.

### Create Python Virtual Environment

> [!IMPORTANT]  
> All code and packages have been tested with Python 3.11

```shell
python -m venv $DEMO_HOME/.venv
```

Install the required packages:

```shell
pip install -r requirements.txt
```

### Slack App Manifest

The Slack App configuration and its manifest are available in [manifest.json](./manifest.json). You can use this while setting up your Slack App. It includes all permissions, commands, and other details needed to interact with Snowflake using the [bot](./app.py).

### Start the Bot

> [!NOTE]  
> Ensure you have installed the Slack App in your workspace using the manifest

```shell
python app.py
```

## References

- [Integrate Snowflake Cortex Analyst REST API with Slack](https://medium.com/snowflake/integrate-snowflake-cortex-analyst-rest-api-with-slack-0b70bde3cb7b)

