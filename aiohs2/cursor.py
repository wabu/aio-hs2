# The MIT License (MIT)
#
# Copyright (c) 2013 Brad Ruderman
# Copyright (c) 2014 Paul Colomiets
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio

from .lowlevel.TCLIService.ttypes import TFetchResultsReq
from .lowlevel.TCLIService.ttypes import TGetResultSetMetadataReq
from .lowlevel.TCLIService.ttypes import TExecuteStatementReq
from .lowlevel.TCLIService.ttypes import TFetchOrientation, TCloseOperationReq
from .lowlevel.TCLIService.ttypes import TGetSchemasReq, TTypeId

from .error import Pyhs2Exception

def get_type(typeDesc):
    for ttype in typeDesc.types:
        if ttype.primitiveEntry is not None:
            return TTypeId._VALUES_TO_NAMES[ttype.primitiveEntry.type]
        elif ttype.mapEntry is not None:
            return ttype.mapEntry
        elif ttype.unionEntry is not None:
            return ttype.unionEntry
        elif ttype.arrayEntry is not None:
            return ttype.arrayEntry
        elif ttype.structEntry is not None:
            return ttype.structEntry
        elif ttype.userDefinedTypeEntry is not None:
            return ttype.userDefinedTypeEntry

def get_value(colValue):
    if colValue.boolVal is not None:
      return colValue.boolVal.value
    elif colValue.byteVal is not None:
      return colValue.byteVal.value
    elif colValue.i16Val is not None:
      return colValue.i16Val.value
    elif colValue.i32Val is not None:
      return colValue.i32Val.value
    elif colValue.i64Val is not None:
      return colValue.i64Val.value
    elif colValue.doubleVal is not None:
      return colValue.doubleVal.value
    elif colValue.stringVal is not None:
      return colValue.stringVal.value

class Cursor(object):
    session = None
    client = None
    operationHandle = None

    def __init__(self, _client, sessionHandle):
        self.session = sessionHandle
        self.client = _client

    @asyncio.coroutine
    def execute(self, hql):
        query = TExecuteStatementReq(self.session, statement=hql, confOverlay={})
        res = yield from self.client.ExecuteStatement(query)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)

    @asyncio.coroutine
    def fetch(self, maxRows=10000):
        rows = []
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=maxRows)
        yield from self._fetch(rows, fetchReq)
        return rows

    def iter(self, maxRows=10000):
        fetchReq = TFetchResultsReq(operationHandle=self.operationHandle,
                                    orientation=TFetchOrientation.FETCH_NEXT,
                                    maxRows=maxRows)
        return self._iter(fetchReq)

    @asyncio.coroutine
    def getSchema(self):
        if self.operationHandle:
            req = TGetResultSetMetadataReq(self.operationHandle)
            res = yield from self.client.GetResultSetMetadata(req)
            if res.schema is not None:
                cols = []
                metadata = yield from self.client.GetResultSetMetadata(req)
                for c in metadata.schema.columns:
                    col = {}
                    col['type'] = get_type(c.typeDesc)
                    col['columnName'] = c.columnName
                    col['comment'] = c.comment
                    cols.append(col)
                return cols
        return None

    @asyncio.coroutine
    def getDatabases(self):
        req = TGetSchemasReq(self.session)
        res = yield from self.client.GetSchemas(req)
        self.operationHandle = res.operationHandle
        if res.status.errorCode is not None:
            raise Pyhs2Exception(res.status.errorCode, res.status.errorMessage)
        return self.fetch()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    @asyncio.coroutine
    def _fetch_chunk(self, fetchReq):
        resultsRes = yield from self.client.FetchResults(fetchReq)
        return [[get_value(col) for col in row.colVals] for row in resultsRes.results.rows]
        
    def _iter(self, fetchReq):
        while True:
            fetch = asyncio.async(self._fetch_chunk(fetchReq))
            yield fetch
            if not fetch.done():
                fetch.cancel()
                raise ValueError("Your loop should use the supplied future.")
            if not fetch.result():
                break

    @asyncio.coroutine
    def _fetch(self, rows, fetchReq):
        while True:
            chunk = yield from self._fetch_chunk(fetchReq)
            rows.extend(chunk)
            if not chunk:
                break
        return rows

    @asyncio.coroutine
    def close(self):
        if self.operationHandle is not None:
            req = TCloseOperationReq(operationHandle=self.operationHandle)
            yield from self.client.CloseOperation(req)
