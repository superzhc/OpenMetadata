#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# pylint: disable=protected-access
"""Oracle source module"""
import traceback
from typing import Iterable, Optional

from sqlalchemy.dialects.oracle.base import INTERVAL, OracleDialect, ischema_names
from sqlalchemy.engine import Inspector

from metadata.generated.schema.entity.data.table import TableType
from metadata.generated.schema.entity.services.connections.database.oracleConnection import (
    OracleConnection,
    OracleDatabaseSchema,
    OracleServiceName,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.column_type_parser import create_sqlalchemy_type
from metadata.ingestion.source.database.common_db_source import (
    CommonDbSourceService,
    TableNameAndType,
)
from metadata.ingestion.source.database.oracle.utils import (
    _get_col_type,
    get_columns,
    get_mview_definition,
    get_mview_names,
    get_mview_names_dialect,
    get_table_comment,
    get_table_names,
    get_view_definition,
)
from metadata.utils.logger import ingestion_logger
from metadata.utils.sqlalchemy_utils import (
    get_all_table_comments,
    get_all_view_definitions,
)

logger = ingestion_logger()


ischema_names.update(
    {
        "ROWID": create_sqlalchemy_type("ROWID"),
        "XMLTYPE": create_sqlalchemy_type("XMLTYPE"),
        "INTERVAL YEAR TO MONTH": INTERVAL,
    }
)

OracleDialect.get_table_comment = get_table_comment
OracleDialect.get_columns = get_columns
OracleDialect._get_col_type = _get_col_type
OracleDialect.get_view_definition = get_view_definition
OracleDialect.get_all_view_definitions = get_all_view_definitions
OracleDialect.get_all_table_comments = get_all_table_comments
OracleDialect.get_table_names = get_table_names
Inspector.get_mview_names = get_mview_names
Inspector.get_mview_definition = get_mview_definition
OracleDialect.get_mview_names = get_mview_names_dialect


class OracleSource(CommonDbSourceService):
    """
    Implements the necessary methods to extract
    Database metadata from Oracle Source
    """

    @classmethod
    def create(cls, config_dict, metadata: OpenMetadata):
        config = WorkflowSource.parse_obj(config_dict)
        connection: OracleConnection = config.serviceConnection.__root__.config
        if not isinstance(connection, OracleConnection):
            raise InvalidSourceException(
                f"Expected OracleConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_database_names(self) -> Iterable[str]:
        """
        2024年4月3日 Oracle 连接类型为数据库方式，则当前连接的数据库即为设置的数据库
        """
        if isinstance(self.service_connection.oracleConnectionType, OracleDatabaseSchema):
            database_name = self.service_connection.oracleConnectionType.databaseSchema
            self.set_inspector(database_name)
            yield database_name
        elif isinstance(self.service_connection.oracleConnectionType, OracleServiceName):
            """
            数据库服务名
            从oracle9i版本开始，引入的一个全新的参数
            如果数据库有域名，则数据库服务名就是全局数据库名；否则，数据库服务名与数据库名相同
            """
            database_name = self.service_connection.oracleConnectionType.oracleServiceName
            self.set_inspector(database_name)
            yield database_name
        else:
            yield from super().get_database_names()

    def get_raw_database_schema_names(self) -> Iterable[str]:
        """
        2024年4月12日 对于oracle来说，schema其实就是当前的用户名称
        """
        yield self.service_connection.username

    def query_table_names_and_types(
        self, schema_name: str
    ) -> Iterable[TableNameAndType]:
        """
        Connect to the source database to get the table
        name and type. By default, use the inspector method
        to get the names and pass the Regular type.

        This is useful for sources where we need fine-grained
        logic on how to handle table types, e.g., external, foreign,...
        """

        regular_tables = [
            TableNameAndType(name=table_name)
            for table_name in self.inspector.get_table_names(schema_name) or []
        ]
        material_tables = [
            TableNameAndType(name=table_name, type_=TableType.MaterializedView)
            for table_name in self.inspector.get_mview_names(schema_name) or []
        ]

        return regular_tables + material_tables

    def get_view_definition(
        self, table_type: str, table_name: str, schema_name: str, inspector: Inspector
    ) -> Optional[str]:
        if table_type not in {TableType.View, TableType.MaterializedView}:
            return None

        definition_fn = inspector.get_view_definition
        if table_type == TableType.MaterializedView:
            definition_fn = inspector.get_mview_definition

        try:
            view_definition = definition_fn(table_name, schema_name)
            view_definition = "" if view_definition is None else str(view_definition)
            return view_definition

        except NotImplementedError:
            logger.warning("View definition not implemented")

        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Failed to fetch view definition for {table_name}: {exc}")
        return None
