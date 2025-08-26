# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
DBAPI 2.0-compatible facade for the ADBC libpq driver.
"""

from adbc_driver_manager.dbapi import Cursor, Connection, _RowIterator

from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pathlib import Path

import tempfile

try:
    import pyarrow
except ImportError as e:
    raise ImportError("PyArrow is required for the DBAPI-compatible interface") from e

try:
    import pyarrow.dataset
except ImportError:
    _pya_dataset = ()
    _pya_scanner = ()
else:
    _pya_dataset = (pyarrow.dataset.Dataset,)
    _pya_scanner = (pyarrow.dataset.Scanner,)

from adbc_driver_manager import _lib, _reader
from adbc_driver_manager._lib import _blocking_call


import adbc_driver_manager
import adbc_driver_manager.dbapi
import adbc_driver_netezza

__all__ = [
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

# ----------------------------------------------------------
# Globals

apilevel = adbc_driver_manager.dbapi.apilevel
threadsafety = adbc_driver_manager.dbapi.threadsafety
# XXX: PostgreSQL doesn't fit any of the param styles
# We'll need some Python-side wrangling specific to this driver
paramstyle = "pyformat"

Warning = adbc_driver_manager.dbapi.Warning
Error = adbc_driver_manager.dbapi.Error
InterfaceError = adbc_driver_manager.dbapi.InterfaceError
DatabaseError = adbc_driver_manager.dbapi.DatabaseError
DataError = adbc_driver_manager.dbapi.DataError
OperationalError = adbc_driver_manager.dbapi.OperationalError
IntegrityError = adbc_driver_manager.dbapi.IntegrityError
InternalError = adbc_driver_manager.dbapi.InternalError
ProgrammingError = adbc_driver_manager.dbapi.ProgrammingError
NotSupportedError = adbc_driver_manager.dbapi.NotSupportedError

# ----------------------------------------------------------
# Types

Date = adbc_driver_manager.dbapi.Date
Time = adbc_driver_manager.dbapi.Time
Timestamp = adbc_driver_manager.dbapi.Timestamp
DateFromTicks = adbc_driver_manager.dbapi.DateFromTicks
TimeFromTicks = adbc_driver_manager.dbapi.TimeFromTicks
TimestampFromTicks = adbc_driver_manager.dbapi.TimestampFromTicks
STRING = adbc_driver_manager.dbapi.STRING
BINARY = adbc_driver_manager.dbapi.BINARY
NUMBER = adbc_driver_manager.dbapi.NUMBER
DATETIME = adbc_driver_manager.dbapi.DATETIME
ROWID = adbc_driver_manager.dbapi.ROWID

INGEST_OPTION_TARGET_FILE_PATH: str
ADBC_NETEZZA_OPTION_FILE_PATH = "adbc.netezza.reader_file_path"
ADBC_NETEZZA_OPTION_ET_OPTIONS = "adbc.netezza.reader_et_options"
# ----------------------------------------------------------
# Functions

def connect(
    uri: str,
    db_kwargs: Optional[Dict[str, str]] = None,
    conn_kwargs: Optional[Dict[str, str]] = None,
    **kwargs
) -> "NetezzaConnection":
    """
    Connect to Netezza via ADBC.

    Parameters
    ----------
    uri : str
        The URI to connect to.
    db_kwargs : dict, optional
        Initial database connection parameters.
    conn_kwargs : dict, optional
        Connection-specific parameters.  (ADBC differentiates between
        a 'database' object shared between multiple 'connection'
        objects.)
    """
    db = None
    conn = None

    try:
        db = adbc_driver_netezza.connect(uri)
        conn = adbc_driver_manager.AdbcConnection(db)
        return NetezzaConnection(db, conn, **kwargs)
    except Exception:
        if conn:
            conn.close()
        if db:
            db.close()
        raise

# ----------------------------------------------------------
# Classes

class NetezzaConnection(Connection):
    def cursor(self) -> "NetezzaCursor":
        """Create a new cursor for querying the database."""
        return NetezzaCursor(self)

class NetezzaCursor(Cursor):
    def __init__(self, conn: NetezzaConnection):
        # Must be at top in case __init__ is interrupted and then __del__ is called
        self._closed = True
        self._conn = conn
        self._stmt = _lib.AdbcStatement(conn._conn)
        self._closed = False

        self._last_query: Optional[Union[str, bytes]] = None
        self._results: Optional["_RowIterator"] = None
        self._arraysize = 1
        self._rowcount = -1
        self.ingest_supported_file_formats = ['csv', 'dat', 'tbl', 'out']
        self.is_temp_dir_created = False

    def check_support(
            self,
            data: Union[pyarrow.RecordBatch, pyarrow.Table, pyarrow.RecordBatchReader], 
            reader_file_path: str
        ) -> bool:
        """
        Checks for support available on Neteza. Currently supporting ingestion of 
        structured table data.

        Parameters
        ----------
        data
            The Arrow data to for the file to insert . 
            This can be a pyarrow RecordBatch, Table or RecordBatchReader, or any Arrow-compatible data that implements
            the Arrow PyCapsule Protocol (i.e. has an ``__arrow_c_array__``
            or ``__arrow_c_stream__`` method).
        reader_file_path
            Netezza specific parameter for adbc_ingest to provide the path
            of the file tryng to ingest
        """
        return isinstance(data, pyarrow.Table) and reader_file_path.split('.')[1] in self.ingest_supported_file_formats

    def adbc_ingest(
        self,
        table_name: str,
        data: Union[pyarrow.RecordBatch, pyarrow.Table, pyarrow.RecordBatchReader],
        mode: Literal["append", "create", "replace", "create_append"] = "create",
        *,
        catalog_name: Optional[str] = None,
        db_schema_name: Optional[str] = None,
        temporary: bool = False,
        reader_file_path: str = None,
        reader_et_options: dict = {}
    ) -> int:
        """Ingest CSV data into a database table.

        For the Netezza Driver this function is used to ingest csv data into
        a database table

        Parameters
        ----------
        table_name
            The table to insert into.
        data
            The Arrow data to for the file to insert . 
            This can be a pyarrow RecordBatch, Table or RecordBatchReader, or any Arrow-compatible data that implements
            the Arrow PyCapsule Protocol (i.e. has an ``__arrow_c_array__``
            or ``__arrow_c_stream__`` method).
        mode
            How to deal with existing data:

            - 'append': append to a table (error if table does not exist)
            - 'create': create a table and insert (error if table exists)
            - 'create_append': create a table (if not exists) and insert
            - 'replace': drop existing table (if any), then same as 'create'
        catalog_name
            If given, the catalog to create/locate the table in.
            **This API is EXPERIMENTAL.**
        db_schema_name
            If given, the schema to create/locate the table in.
            **This API is EXPERIMENTAL.**
        temporary
            Whether to ingest to a temporary table or not.  Most drivers will
            not support setting this along with catalog_name and/or
            db_schema_name.
            **This API is EXPERIMENTAL.**
        reader_file_path
            Netezza specific parameter for adbc_ingest to provide the path
            of the file tryng to ingest
            **This API is EXPERIMENTAL.**
        reader_et_options
            Netezza specific parameter which can be used to specify the parameters
            for etxernal table parameters as a dictionary
            The options can be discovered from :
            https://www.ibm.com/docs/en/netezza?topic=eto-option-summary
            **This API is EXPERIMENTAL.**

        Returns
        -------
        int
            The number of rows inserted, or -1 if the driver cannot
            provide this information.

        Notes
        -----
        This is an extension and not part of the DBAPI standard.

        """
        # Check if pyarrow table instance is provided, if yes then create a temp path
        # and set default reader_et_options
        if isinstance(data, pyarrow.Table) and reader_file_path is None:
            tempdir = tempfile.TemporaryDirectory(
                prefix="adbc-docs-",
                ignore_cleanup_errors=True,
            )
            root = Path(tempdir.name)
            reader_file_path = str(root / "example.csv")
            pyarrow.csv.write_csv(data, reader_file_path)
            reader_et_options = {"delim" : "','", "MaxErrors":0, "SkipRows":1}
            self.is_temp_dir_created = True
        if not self.check_support(data, reader_file_path):
            raise ValueError("Not supported on Netezza yet..")
        if mode == "append":
            c_mode = _lib.INGEST_OPTION_MODE_APPEND
        elif mode == "create":
            c_mode = _lib.INGEST_OPTION_MODE_CREATE
        elif mode == "create_append":
            c_mode = _lib.INGEST_OPTION_MODE_CREATE_APPEND
        elif mode == "replace":
            c_mode = _lib.INGEST_OPTION_MODE_REPLACE
        else:
            raise ValueError(f"Invalid value for 'mode': {mode}")

        reader_et_options = " ".join([f"{key} {reader_et_options[key]}" for key in reader_et_options])

        options = {
            _lib.INGEST_OPTION_TARGET_TABLE: table_name,
            _lib.INGEST_OPTION_MODE: c_mode,
            ADBC_NETEZZA_OPTION_FILE_PATH : reader_file_path,
            ADBC_NETEZZA_OPTION_ET_OPTIONS : reader_et_options
        }
        if catalog_name is not None:
            options[
                adbc_driver_manager.StatementOptions.INGEST_TARGET_CATALOG.value
            ] = catalog_name
        if db_schema_name is not None:
            options[
                adbc_driver_manager.StatementOptions.INGEST_TARGET_DB_SCHEMA.value
            ] = db_schema_name

        self._stmt.set_options(**options)

        if temporary:
            self._stmt.set_options(
                **{
                    adbc_driver_manager.StatementOptions.INGEST_TEMPORARY.value: "true",
                }
            )
        else:
            # Need to explicitly clear it, but not all drivers support this
            options = {
                adbc_driver_manager.StatementOptions.INGEST_TEMPORARY.value: "false",
            }
            try:
                self._stmt.set_options(**options)
            except NotSupportedError:
                pass

        if hasattr(data, "__arrow_c_array__"):
            self._stmt.bind(data)
        elif hasattr(data, "__arrow_c_stream__"):
            self._stmt.bind_stream(data)
        elif isinstance(data, pyarrow.RecordBatch):
            array = _lib.ArrowArrayHandle()
            schema = _lib.ArrowSchemaHandle()
            data._export_to_c(array.address, schema.address)
            self._stmt.bind(array, schema)
        else:
            if isinstance(data, pyarrow.Table):
                data = data.to_reader()
            elif isinstance(data, pyarrow.dataset.Dataset):
                data = data.scanner().to_reader()
            elif isinstance(data, pyarrow.dataset.Scanner):
                data = data.to_reader()
            elif not hasattr(data, "_export_to_c"):
                data = pyarrow.Table.from_batches(data)
                data = data.to_reader()
            handle = _lib.ArrowArrayStreamHandle()
            data._export_to_c(handle.address)
            self._stmt.bind_stream(handle)

        self._last_query = None
        result = _blocking_call(self._stmt.execute_update, (), {}, self._stmt.cancel)
        if self.is_temp_dir_created:
            tempdir.cleanup()
        return result

Connection = NetezzaConnection
Cursor = NetezzaCursor
