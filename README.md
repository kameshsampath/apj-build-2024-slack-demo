# Cortex Analyst with Slack

A simple demo showcasing the integration of Snowflake Cortex Analyst with Slack. The demo also covers ChatOps aspects using the Snowflake Python API for setting up Snowflake resources such as databases, tables, stages, etc.

## Prerequisites

- Snowflake Account (get a free trial from https://signup.snowflake.com if needed)
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) to setup your Snowflake connection locally. This will be used by the Slack-bot application when connecting to Snowflake.
- Snowflake [KeyPair Authentication](https://docs.snowflake.com/user-guide/key-pair-auth#configuring-key-pair-authentication) setup, as REST API will use JWT for secure communication
- A Slack Workspace where you can install this Slack-bot app. [Learn how to create a Slack workspace](https://slack.com/intl/en-in/help/articles/206845317-Create-a-Slack-workspace)
- Slack tokens setup - Follow the [Bolt Python getting started guide](https://tools.slack.dev/bolt-python/getting-started#tokens-and-installing-apps)

> [!NOTE]  
> Check out [direnv](https://direnv.net/) for a neat and clean way to manage your environment variables.

## Setup

Configure all required environment variables using `.env`:

```shell
cp $DEMO_HOME/.env.example $DEMO/.env
```

Be sure to source the `.env` file for local use with `snow`. The `app.py` automatically loads it if it exists.

## Create Python Virtual Environment

> [!IMPORTANT]  
> All code and packages have been tested with Python 3.11

```shell
python -m venv $DEMO_HOME/.venv
```

Install the required packages:

```shell
pip install -r requirements.txt
```

## Slack App Manifest

The Slack App configuration and its manifest are available in [manifest.json](./manifest.json). You can use this while setting up your Slack App. It includes all permissions, commands, and other details needed to interact with Snowflake using the [bot](./app.py).

## Start the Bot

> [!NOTE]  
> Ensure you have installed the Slack App in your workspace using the manifest

```shell
python app.py
```

## App Demo

[![Demo Video](https://img.youtube.com/vi/IwLrV_hJtuE/0.jpg)](https://www.youtube.com/watch?v=IwLrV_hJtuE)

## References

- [Integrate Snowflake Cortex Analyst REST API with Slack](https://medium.com/snowflake/integrate-snowflake-cortex-analyst-rest-api-with-slack-0b70bde3cb7b)
