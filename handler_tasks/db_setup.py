import logging
import os
from datetime import datetime, timedelta, timezone

from jinja2 import Environment, FileSystemLoader
from snowflake.core import CreateMode, Root
from snowflake.core.database import Database
from snowflake.core.pipe import Pipe
from snowflake.core.schema import Schema
from snowflake.core.stage import Stage, StageDirectoryTable, StageEncryption
from snowflake.core.table import Table, TableColumn

from slack_sdk import WebClient


class DBSetup:
    """
    A utility class to set up required Snowflake database objects and resources for the demo.

    This class provides methods to create and configure various Snowflake objects including
    database, schema, file formats, stages, and tables specifically for handling support
    ticket data.

    Methods:
        create_db(db_name: str) -> None:
            Creates a new database with the specified name.

        create_schema(schema_name: str, db_name: Database) -> None:
            Creates a new schema in the specified database.

        create_file_formats(db_name: str, schema_name: str):
            Creates CSV file format definitions for data loading.

        create_stage(db_name: str, schema_name: str, stage_name: str = "support_tickets_data") -> None:
            Creates a stage for temporarily storing data files.

        create_table(db_name: str, schema_name: str, table_name: str = "support_tickets") -> None:
            Creates the support tickets table with required schema.
    """

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    _mode = CreateMode.if_not_exists

    def __init__(
        self,
        session,
        db_name: str = "demo_db",
        schema_name: str = "data",
        semantic_models_stage: str = "semantic_models",
        semantic_model_file: str = "support_tickets_semantic_model.yaml",
    ):
        self.session = session
        self.root = Root(session)
        self._db_name = db_name
        self._schema_name = schema_name
        self._semantic_models_stage = semantic_models_stage
        self._semantic_model_file = semantic_model_file

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, db_name: str):
        self._db_name = db_name

    @property
    def schema_name(self):
        return self._schema_name

    @schema_name.setter
    def schema_name(self, schema_name: str):
        self._schema_name = schema_name

    @property
    def semantic_models_stage(self):
        return self._semantic_models_stage

    @semantic_models_stage.setter
    def semantic_models_stage(self, semantic_models_stage: str):
        self._semantic_models_stage = semantic_models_stage

    @property
    def semantic_model_file(self):
        return self._semantic_model_file

    @semantic_model_file.setter
    def semantic_model_file(self, semantic_model_file: str):
        self._semantic_model_file = semantic_model_file

    def create_db(self, db_name: str) -> None:
        """
        Create the database that will be used in the demo.

        Args:
            db_name (str): The name of the database to create.

        Raises:
            Exception: for any errors
        """

        self.LOGGER.debug(f"Creating database {db_name}")
        database = Database(db_name, comment="created by slack bot setup")
        try:
            self.root.databases[db_name].create_or_alter(database)
        except Exception as e:
            self.LOGGER.error(e)
            raise f"Error creating database {db_name},{e}"

    def create_schema(self, schema_name: str, db_name: Database) -> None:
        """
        Create the Schema for the demo.

        Args:
            schema_name (str): Name of the schema to create.
            db_name (Database): Database object where schema will be created.

        Raises:
            Exception: for any errors
        """
        self.LOGGER.debug(f"Creating Schema {schema_name}")
        schema = Schema(schema_name, comment="created by slack bot setup")
        try:

            self.root.databases[db_name].schemas.create(
                schema=schema,
                mode=self._mode,
            )
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating schema {schema_name},{e}")

    def create_file_formats(
        self,
        db_name: str,
        schema_name: str,
    ):
        """
        Create the CSV File Format used for data loading.

        Args:
            db_name (str): Name of the database where file format will be created.
            schema_name (str): Name of the schema where file format will be created.

        Raises:
            Exception: for any errors
        """
        try:
            ff_name = ".".join([db_name, schema_name, "csvformat"])
            self.LOGGER.debug(f"Creating file format {ff_name}")
            df = self.session.sql(
                f"""CREATE OR REPLACE FILE FORMAT {ff_name}
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TYPE = 'CSV'
COMMENT = 'created by slack bot setup';
"""
            )
            df.collect()
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating file format {ff_name},{e}")

    def create_stage(
        self,
        db_name: str,
        schema_name: str,
        stage_name: str = "support_tickets_data",
    ) -> None:
        """
        Create the stage used for the demo.

        Args:
            db_name (str): Name of the database where stage will be created.
            schema_name (str): Name of the schema where stage will be created.
            stage_name (str, optional): Name of the stage. Defaults to "support_tickets_data".

        Raises:
            Exception: for any errors
        """
        try:
            stages = [
                Stage(
                    name=stage_name,
                    url="s3://sfquickstarts/finetuning_llm_using_snowflake_cortex_ai/",
                    comment="created by slack bot setup",
                    directory_table=StageDirectoryTable(enable=True),
                ),
                Stage(
                    name=f"older_than_7days_{stage_name}",
                    encryption=StageEncryption(type="SNOWFLAKE_SSE"),
                    directory_table=StageDirectoryTable(enable=True),
                    comment="created by slack bot setup",
                ),
                Stage(
                    name=self.semantic_models_stage,
                    encryption=StageEncryption(type="SNOWFLAKE_SSE"),
                    directory_table=StageDirectoryTable(enable=True),
                    comment="created by slack bot setup",
                ),
            ]
            for stage in stages:
                (
                    self.root.databases[db_name]
                    .schemas[schema_name]
                    .stages.create(
                        stage,
                        mode=self._mode,
                    )
                )
            # upload the semantic model file
            curr_path = os.path.abspath(os.path.dirname(__file__))
            template_dir = os.path.join(
                curr_path,
                "..",
                "data",
            )
            # make the absolute path
            template_dir = os.path.abspath(template_dir)
            # just the template file alone, it will be loaded relative to template dir
            _model_file_template = f"{self.semantic_model_file}.j2"
            env = Environment(
                loader=FileSystemLoader(
                    template_dir
                ),  # Look for templates in 'data' directory
                trim_blocks=True,
                lstrip_blocks=True,
            )

            _model_file = os.path.join(
                template_dir,
                self.semantic_model_file,
            )

            template = env.get_template(_model_file_template)
            rendered_yaml = template.render(
                {"db_name": db_name, "schema_name": schema_name}
            )
            with open(_model_file, "w") as file:
                file.write(rendered_yaml)

            self.LOGGER.debug(
                f"Uploading semantic model {_model_file} to stage '{self.semantic_models_stage}'"
            )
            self.root.databases[db_name].schemas[schema_name].stages[
                self.semantic_models_stage
            ].put(
                _model_file,
                stage_location="/",
                auto_compress=False,
                overwrite=True,
            )
        except Exception as e:
            self.LOGGER.error(e, exc_info=True)
            raise Exception(f"Error creating stages,{e}")

    def create_table(
        self,
        db_name: str,
        schema_name: str,
        table_name: str = "support_tickets",
    ) -> None:
        """
        Create the table that will be used in the demo.

        Args:
            db_name (str): Name of the database where table will be created.
            schema_name (str): Name of the schema where table will be created.
            table_name (str, optional): Name of the table. Defaults to "support_tickets".

        Raises:
            Exception: for any errors
        """
        try:
            table_columns = [
                TableColumn(
                    name="ticket_id",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="customer_name",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="customer_email",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="service_type",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="request",
                    datatype="varchar",
                ),
                TableColumn(
                    name="contact_preference",
                    datatype="varchar(60)",
                ),
            ]
            table = Table(
                name=table_name,
                columns=table_columns,
                comment="created by slack bot setup",
            )
            (
                self.root.databases[db_name]
                .schemas[schema_name]
                .tables.create(
                    table,
                    mode=self._mode,
                )
            )
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating table {table_name},{e}")

    def is_date_older_than_7days(self, date_str):
        """
        Check if a given date string represents a date that is older than 7 days from the current date.

        Args:
            date_str (str): Date string to check, expected in a format parseable by Snowflake
                           (e.g., 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MI:SS')

        Returns:
            bool: True if the date is more than 7 days old, False otherwise

        Raises:
            Exception: for any errors
        """
        # Parse the date string into datetime object
        date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")

        # Get current time in UTC
        current_time = datetime.now(timezone.utc)

        # Calculate the difference
        time_difference = current_time - date_obj.replace(tzinfo=timezone.utc)

        # Check if difference is more than 7 days
        return time_difference > timedelta(days=7)

    def pipe_and_load(
        self,
        db_name: str,
        schema_name: str,
        stage_name: str = "support_tickets_data",
        table_name: str = "support_tickets",
        ff_name: str = "csvformat",
        pipe_name: str = "support_tickets_data",
    ) -> None:
        """
        Create a Snowflake Pipe and load data from the external stage into the target table.
        This method sets up automated data ingestion by creating a Snowpipe that monitors
        the specified stage for new files and automatically loads them into the table.

        Args:
            db_name (str): Name of the database where the pipe will be created
            schema_name (str): Name of the schema where the pipe will be created
            stage_name (str, optional): Name of the external stage containing the data files.
                                    Defaults to "support_tickets_data"
            table_name (str, optional): Name of the target table for data loading.
                                    Defaults to "support_tickets"
            ff_name (str, optional): Name of the file format to use for data loading.
                                Defaults to "csvformat"
            pipe_name (str, optional): Name to assign to the created pipe.
                                    Defaults to "support_tickets_data"

        Raises:
            Exception: If there are issues creating the pipe
                or if the specified database/schema/stage/table doesn't exist

        Note:
            - The pipe will automatically handle file loading once created
            - Ensure the stage and file format are properly configured before creating the pipe
        """
        try:
            self.LOGGER.debug("Pipe and Load")

            _table_fqn = f"{db_name}.{schema_name}.{table_name}"
            _stage_fqn = f"{db_name}.{schema_name}.{stage_name}"
            _target_stage_fqn = f"{db_name}.{schema_name}.older_than_7days_{stage_name}"
            ff_fqn = f"{db_name}.{schema_name}.{ff_name}"

            support_tickets_pipes = [
                Pipe(
                    name=pipe_name,
                    auto_ingest=True,
                    comment="created by slack bot setup",
                    copy_statement=f"COPY INTO {_table_fqn} FROM @{_stage_fqn}/ FILE_FORMAT = (FORMAT_NAME = '{ff_fqn}')",
                ),
                Pipe(
                    name=f"older_than_7days_{pipe_name}",
                    comment="created by slack bot setup to ingest 7 days older data files from external stage",
                    copy_statement=f"COPY INTO {_table_fqn} FROM @{_target_stage_fqn} FILE_FORMAT = (FORMAT_NAME = '{ff_fqn}')",
                ),
            ]

            pipes = self.root.databases[db_name].schemas[schema_name].pipes

            for pipe in support_tickets_pipes:
                pipes.create(
                    pipe,
                    mode=self._mode,
                )
            # handle files older than 7 days (!!!IMPORTANT!!! Only for Demos)
            self.LOGGER.debug("Handle files greater than 7 days, just for demo.")
            old_stage_files = (
                self.root.databases[db_name]
                .schemas[schema_name]
                .stages[stage_name]
                .list_files(pattern=".*[.csv]")
            )
            older_than_7days = [
                os.path.basename(f.name)
                for f in old_stage_files
                if self.is_date_older_than_7days(f.last_modified)
            ]

            if len(older_than_7days) > 0:
                _older_files = ",".join(f"'{x}'" for x in older_than_7days)
                self.LOGGER.debug(
                    f"Files Older than 7 days to be loaded to internal stage {_older_files}"
                )
                self.session.sql(
                    f"""
                COPY FILES 
                INTO @{_target_stage_fqn}
                FROM @{_stage_fqn}
                FILES=({_older_files})
                    """
                ).collect()
                # trigger run
                _pipe = (
                    self.root.databases[db_name]
                    .schemas[schema_name]
                    .pipes[f"older_than_7days_{pipe_name}"]
                )
                _pipe.refresh()

        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating pipe and loading data,{e}")

    def do(self, client: WebClient, channel_id: str) -> None:
        """
        Creates or alters Snowflake Database objects using the Snowflake Python API.
        This method serves as the main orchestrator for setting up all required database
        objects in the correct order.

        The method performs the following setup steps:
        1. Creates the database if it doesn't exist
        2. Creates the schema if it doesn't exist
        3. Creates or replaces file formats for data loading
        4. Creates or replaces the stage for data files
        5. Creates the support tickets table if it doesn't exist
        6. Sets up the data loading pipe

        Raises:
           Exception: If there are any issues creating
                or altering the database objects

        Note:
            - Existing objects will be preserved where possible (using IF NOT EXISTS)
            - File formats and stages will be replaced if they exist
            - This method should be called after initializing the DBSetup class with
            a valid Snowflake connection
        """

        try:
            self.LOGGER.debug(
                f"Using Database : {self.db_name} and Schema : {self.schema_name}"
            )

            self.create_db(self.db_name)
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark: Created database {self.db_name}",
            )
            self.create_schema(
                schema_name=self.schema_name,
                db_name=self.db_name,
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark:  Created schema {self.schema_name}",
            )
            self.create_file_formats(
                db_name=self.db_name,
                schema_name=self.schema_name,
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark:  Created file formats.",
            )
            self.create_stage(
                db_name=self.db_name,
                schema_name=self.schema_name,
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark:  Created stages.",
            )
            self.create_table(
                db_name=self.db_name,
                schema_name=self.schema_name,
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark:  Created tables.",
            )
            self.pipe_and_load(
                db_name=self.db_name,
                schema_name=self.schema_name,
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f":white_check_mark: Loaded data, wait for 15 to 20 secs for the pipe to refresh and ingest data.",
            )
            client.chat_postMessage(
                channel=channel_id,
                text=f"Setup completed.",
            )
        except Exception as e:
            self.LOGGER.error(
                "Error setting up demo",
                exc_info=True,
            )
            raise Exception(f"Error setting up demo,{e}")
