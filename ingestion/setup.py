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

"""
Python Dependencies
"""

import os
from typing import Dict, Set

from setuptools import find_namespace_packages, setup


def get_long_description():
    root = os.path.dirname(__file__)
    with open(os.path.join(root, "README.md"), encoding="UTF-8") as file:
        description = file.read()
    return description


# Add here versions required for multiple plugins
VERSIONS = {
    "airflow": "apache-airflow==2.6.3",
    "avro": "avro~=1.11",
    "boto3": "boto3>=1.20,<2.0",  # No need to add botocore separately. It's a dep from boto3
    "geoalchemy2": "GeoAlchemy2~=0.12",
    "google-cloud-storage": "google-cloud-storage==1.43.0",
    "great-expectations": "great-expectations~=0.17.0",
    "grpc-tools": "grpcio-tools>=1.47.2",
    "msal": "msal~=1.2",
    "neo4j": "neo4j~=5.3.0",
    "pandas": "pandas==1.3.5",
    "pyarrow": "pyarrow~=10.0",
    "pydomo": "pydomo~=0.3",
    "pymysql": "pymysql>=1.0.2",
    "pyodbc": "pyodbc>=4.0.35,<5",
    "scikit-learn": "scikit-learn~=1.0",  # Python 3.7 only goes up to 1.0.2
    "packaging": "packaging==21.3",
    "azure-storage-blob": "azure-storage-blob~=12.14",
    "azure-identity": "azure-identity~=1.12",
    "sqlalchemy-databricks": "sqlalchemy-databricks~=0.1",
    "databricks-sdk": "databricks-sdk~=0.1",
    "google": "google>=3.0.0",
    "trino": "trino[sqlalchemy]",
    "spacy": "spacy==3.5.0",
    "looker-sdk": "looker-sdk>=22.20.0",
    "lkml": "lkml~=1.3",
    "tableau": "tableau-api-lib~=0.1",
    "pyhive": "pyhive~=0.7",
    "mongo": "pymongo~=4.3",
    "redshift": "sqlalchemy-redshift==0.8.12",
    "snowflake": "snowflake-sqlalchemy~=1.4",
    "elasticsearch8": "elasticsearch8~=8.9.0",
    "giturlparse": "giturlparse",
}

COMMONS = {
    "datalake": {
        VERSIONS["boto3"],
        VERSIONS["pandas"],
        VERSIONS["pyarrow"],
        "python-snappy~=0.6.1",
    },
    "hive": {
        "presto-types-parser>=0.0.2",
        VERSIONS["pyhive"],
    },
    "kafka": {
        VERSIONS["avro"],
        "confluent_kafka==2.1.1",
        "fastavro>=1.2.0",
        # Due to https://github.com/grpc/grpc/issues/30843#issuecomment-1303816925
        # use >= v1.47.2 https://github.com/grpc/grpc/blob/v1.47.2/tools/distrib/python/grpcio_tools/grpc_version.py#L17
        VERSIONS[
            "grpc-tools"
        ],  # grpcio-tools already depends on grpcio. No need to add separately
        "protobuf",
    },
}

# required library for pii tagging
pii_requirements = {
    VERSIONS["spacy"],
    VERSIONS["pandas"],
    "presidio-analyzer==2.2.32",
}

base_requirements = {
    "antlr4-python3-runtime==4.9.2",
    VERSIONS["avro"],  # Used in sample data
    VERSIONS["boto3"],  # Required in base for the secrets manager
    "cached-property==1.5.2",
    "chardet==4.0.0",
    "croniter~=1.3.0",
    "cryptography",
    "email-validator>=1.0.3",
    VERSIONS["google"],
    "google-auth>=1.33.0",
    VERSIONS["grpc-tools"],  # Used in sample data
    "idna<3,>=2.5",
    "importlib-metadata>=4.13.0",  # From airflow constraints
    "Jinja2>=2.11.3",
    "jsonpatch==1.32",
    "jsonschema",
    "memory-profiler",
    "mypy_extensions>=0.4.3",
    "pydantic~=1.10",
    VERSIONS["pymysql"],
    "python-dateutil>=2.8.1",
    "python-jose~=3.3",
    "PyYAML~=6.0",
    "requests>=2.23",
    "requests-aws4auth~=1.1",  # Only depends on requests as external package. Leaving as base.
    "setuptools~=66.0.0",
    "sqlalchemy>=1.4.0,<2",
    "collate-sqllineage>=1.0.4",
    "tabulate==0.9.0",
    "typing-compat~=0.1.0",  # compatibility requirements for 3.7
    "typing_extensions<=4.5.0",  # We need to have this fixed due to a yanked release 4.6.0
    "typing-inspect",
    "wheel~=0.38.4",
    "sqllineage==1.4.9",
}


plugins: Dict[str, Set[str]] = {
    "airflow": {VERSIONS["airflow"]},  # Same as ingestion container. For development.
    "amundsen": {VERSIONS["neo4j"]},
    "athena": {"pyathena==3.0.8"},
    "atlas": {},
    "azuresql": {VERSIONS["pyodbc"]},
    "azure-sso": {VERSIONS["msal"]},
    "backup": {VERSIONS["boto3"], "azure-identity", "azure-storage-blob"},
    "bigquery": {
        "cachetools",
        "google-cloud-datacatalog>=3.6.2",
        "google-cloud-logging",
        VERSIONS["pyarrow"],
        "sqlalchemy-bigquery>=1.2.2",
    },
    "clickhouse": {"clickhouse-driver~=0.2", "clickhouse-sqlalchemy~=0.2"},
    "dagster": {
        VERSIONS["pymysql"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
        "dagster_graphql~=1.1",
    },
    "dbt": {
        "google-cloud",
        VERSIONS["boto3"],
        VERSIONS["google-cloud-storage"],
        "dbt-artifacts-parser",
        VERSIONS["azure-storage-blob"],
        VERSIONS["azure-identity"],
    },
    "db2": {"ibm-db-sa~=0.3"},
    "databricks": {VERSIONS["sqlalchemy-databricks"], VERSIONS["databricks-sdk"]},
    "datalake-azure": {
        VERSIONS["azure-storage-blob"],
        VERSIONS["azure-identity"],
        "adlfs>=2022.2.0",  # Python 3.7 does only support up to 2022.2.0
        *COMMONS["datalake"],
    },
    "datalake-gcs": {
        VERSIONS["google-cloud-storage"],
        "gcsfs==2022.11.0",
        *COMMONS["datalake"],
    },
    "datalake-s3": {
        # requires aiobotocore
        # https://github.com/fsspec/s3fs/blob/9bf99f763edaf7026318e150c4bd3a8d18bb3a00/requirements.txt#L1
        # however, the latest version of `s3fs` conflicts its `aiobotocore` dep with `boto3`'s dep on `botocore`.
        # Leaving this marked to the automatic resolution to speed up installation.
        "s3fs==0.4.2",
        *COMMONS["datalake"],
    },
    "deltalake": {"delta-spark<=2.3.0"},
    "docker": {"python_on_whales==0.55.0"},
    "domo": {VERSIONS["pydomo"]},
    "doris": {"pydoris==1.0.2"},
    "druid": {"pydruid>=0.6.5"},
    "dynamodb": {VERSIONS["boto3"]},
    "elasticsearch": {
        "elasticsearch==7.13.1",
        VERSIONS["elasticsearch8"],
    },  # also requires requests-aws4auth which is in base
    "glue": {VERSIONS["boto3"]},
    "great-expectations": {VERSIONS["great-expectations"]},
    "hive": {
        *COMMONS["hive"],
        "thrift>=0.13,<1",
        "sasl~=0.3",
        "thrift-sasl~=0.4",
        "impyla~=0.18.0",
    },
    "impala": {
        "presto-types-parser>=0.0.2",
        "impyla[kerberos]~=0.18.0",
        "thrift>=0.13,<1",
        "sasl~=0.3",
        "thrift-sasl~=0.4",
    },
    "kafka": {*COMMONS["kafka"]},
    "kinesis": {VERSIONS["boto3"]},
    "ldap-users": {"ldap3==2.9.1"},
    "looker": {
        VERSIONS["looker-sdk"],
        VERSIONS["lkml"],
        "gitpython~=3.1.34",
        VERSIONS["giturlparse"],
    },
    "mlflow": {"mlflow-skinny>=2.3.0", "alembic~=1.10.2"},
    "mongo": {VERSIONS["mongo"], VERSIONS["pandas"]},
    "couchbase": {"couchbase~=4.1"},
    "mssql": {"sqlalchemy-pytds~=0.3"},
    "mssql-odbc": {VERSIONS["pyodbc"]},
    "mysql": {VERSIONS["pymysql"]},
    "nifi": {},  # uses requests
    "okta": {"okta~=2.3"},
    "oracle": {"cx_Oracle>=8.3.0,<9", "oracledb~=1.2"},
    "pgspider": {"psycopg2-binary", "sqlalchemy-pgspider"},
    "pinotdb": {"pinotdb~=0.3"},
    "postgres": {
        VERSIONS["pymysql"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
        VERSIONS["packaging"],
    },
    "powerbi": {VERSIONS["msal"]},
    "qliksense": {"websocket-client~=1.6.1"},
    "presto": {*COMMONS["hive"]},
    "pymssql": {"pymssql~=2.2.0"},
    "quicksight": {VERSIONS["boto3"]},
    "redash": {VERSIONS["packaging"]},
    "redis": {"redis==5.0.8"},
    "redpanda": {*COMMONS["kafka"]},
    "redshift": {
        # Going higher has memory and performance issues
        VERSIONS["redshift"],
        "psycopg2-binary",
        VERSIONS["geoalchemy2"],
    },
    "sagemaker": {VERSIONS["boto3"]},
    "salesforce": {"simple_salesforce==1.11.4"},
    "sap-hana": {"hdbcli", "sqlalchemy-hana"},
    "singlestore": {VERSIONS["pymysql"]},
    "sklearn": {VERSIONS["scikit-learn"]},
    "snowflake": {VERSIONS["snowflake"]},
    "superset": {},  # uses requests
    "tableau": {VERSIONS["tableau"]},
    "trino": {VERSIONS["trino"]},
    "vertica": {"sqlalchemy-vertica[vertica-python]>=0.0.5"},
    "pii-processor": pii_requirements,
    "http": {},  # uses requests
    "mqtt": {"paho-mqtt"}
}

dev = {
    "black==22.3.0",
    "datamodel-code-generator==0.22.0",
    "docker",
    "isort",
    "pre-commit",
    "pycln",
    "pylint~=3.0.0",
    "twine",
}

test = {
    # Install Airflow as it's not part of `all` plugin
    VERSIONS["airflow"],
    "coverage",
    # Install GE because it's not in the `all` plugin
    VERSIONS["great-expectations"],
    "moto==4.0.8",
    "pytest==7.0.0",
    "pytest-cov",
    "pytest-order",
    # install dbt dependency
    "dbt-artifacts-parser",
    VERSIONS["sqlalchemy-databricks"],
    VERSIONS["databricks-sdk"],
    VERSIONS["google"],
    VERSIONS["scikit-learn"],
    VERSIONS["pyarrow"],
    VERSIONS["trino"],
    VERSIONS["spacy"],
    VERSIONS["pydomo"],
    VERSIONS["looker-sdk"],
    VERSIONS["lkml"],
    VERSIONS["tableau"],
    VERSIONS["pyhive"],
    VERSIONS["mongo"],
    VERSIONS["redshift"],
    VERSIONS["snowflake"],
    VERSIONS["elasticsearch8"],
    VERSIONS["giturlparse"],
}

e2e_test = {
    # playwright dependencies
    "pytest-playwright",
    "pytest-base-url",
}

build_options = {"includes": ["_cffi_backend"]}
setup(
    name="openmetadata-ingestion",
    version="1.2.0.1",
    url="https://open-metadata.org/",
    author="OpenMetadata Committers",
    license="Apache License 2.0",
    description="Ingestion Framework for OpenMetadata",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
    options={"build_exe": build_options},
    package_dir={"": "src"},
    package_data={"metadata.examples": ["workflows/*.yaml"]},
    zip_safe=False,
    dependency_links=[],
    project_urls={
        "Documentation": "https://docs.open-metadata.org/",
        "Source": "https://github.com/open-metadata/OpenMetadata",
    },
    packages=find_namespace_packages(where="./src", exclude=["tests*"]),
    namespace_package=["metadata"],
    entry_points={
        "console_scripts": ["metadata = metadata.cmd:metadata"],
        "apache_airflow_provider": [
            "provider_info = airflow_provider_openmetadata:get_provider_config"
        ],
    },
    install_requires=list(base_requirements),
    extras_require={
        "base": list(base_requirements),
        "dev": list(dev),
        "test": list(test),
        "e2e_test": list(e2e_test),
        "data-insight": list(plugins["elasticsearch"]),
        **{plugin: list(dependencies) for (plugin, dependencies) in plugins.items()},
        "all": list(
            base_requirements.union(
                *[
                    requirements
                    for plugin, requirements in plugins.items()
                    if plugin not in {"airflow", "db2", "great-expectations"}
                ]
            )
        ),
        "xgit": list(
            base_requirements.union(
                *[
                    requirements
                    for plugin, requirements in plugins.items()
                    if plugin in {
                        "pii-processor",
                        # database
                        "clickhouse",
                        "dbt",
                        "doris",
                        "mongo",
                        "mssql",
                        "mssql-odbc",
                        "oracle",
                        "postgres",
                        "pymssql",
                        # messaging
                        "kafka",
                        # network
                        "redis",
                        "http",
                        "mqtt"
                    }
                ]
            )
        ),
    },
)
