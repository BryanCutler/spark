#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pyarrow as pa

from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.serializers import NoOpSerializer, ArrowCollectSerializer, ArrowStreamSerializer, \
    FramedSerializer
from pyspark.sql.types import from_arrow_schema, to_arrow_schema
from pyspark.traceback_utils import SCCallSiteSync
from pyspark.rdd import _load_from_socket


@property
def as_arrow(self):
    return ArrowDataFrame(self, pandas_mode=False)


@property
def as_pandas(self):
    return ArrowDataFrame(self, pandas_mode=True)


def to_arrow_iterator(self, batch_order_fn=None):

    with SCCallSiteSync(self._sc) as css:
        sock_info = self._jdf.collectAsArrowToPython()

    it = _load_from_socket(sock_info, ArrowCollectSerializer())
    for result in it:
        # TODO: maybe better to check if last in sequence?
        if isinstance(result, pa.RecordBatch):
            yield result
        elif batch_order_fn is not None:
            batch_order_fn(result)


def patch_spark():
    from pyspark.sql import DataFrame
    DataFrame.as_arrow = as_arrow
    DataFrame.as_pandas = as_pandas
    DataFrame.to_arrow_iterator = to_arrow_iterator


def _wrap_pandas_func(f, preserve_index=False):

    def process_pandas(*batches):
        return pa.RecordBatch.from_pandas(
            f(*(batch.to_pandas() for batch in batches)), preserve_index=preserve_index)

    return process_pandas


class ArrowRDDBase(object):
    """
    Base class to provide RDD-like operations with ``pandas.DataFrame``.
    """

    def __init__(self, sql_ctx, pandas_mode):
        self._sql_ctx = sql_ctx
        self._pandas_mode = pandas_mode

    def _wrap_rdd(self, rdd):
        rdd._jrdd_deserializer = ArrowFramedSerializer()
        return ArrowRDD(rdd, self._sql_ctx, pandas_mode=self._pandas_mode)

    @property
    def as_arrow(self):
        self._pandas_mode = False
        return self

    @property
    def as_pandas(self):
        self._pandas_mode = True
        return self

    @property
    def _rdd(self):
        raise NotImplementedError

    def map(self, f, preserves_partitioning=False, preserve_index=False):
        raise NotImplementedError

    def reduce(self, f, preserve_index=False):
        f_wrapped = _wrap_pandas_func(f, preserve_index) if self._pandas_mode else f

        def func(iterator):
            iterator = iter(iterator)
            try:
                initial = next(iterator)
            except StopIteration:
                return
            yield reduce(f_wrapped, iterator, initial)

        vals = self._wrap_rdd(self._rdd.mapPartitions(func)).collect()
        if vals:
            return reduce(f, vals)
        raise ValueError("Can not reduce() empty RDD")

    def count(self):
        return self._rdd.count()

    def collect(self):
        results = self._rdd.collect()
        return [batch.to_pandas() for batch in results] if self._pandas_mode else results


class ArrowRDD(ArrowRDDBase):
    """
    Wraps a Python RDD to deserialize using Arrow into ``pandas.DataFrame`` for processing.
    """

    def __init__(self, rdd, sql_ctx, pandas_mode):
        super(ArrowRDD, self).__init__(sql_ctx, pandas_mode)
        self._rdd_obj = rdd

    def map(self, f, preserves_partitioning=False, preserve_index=False):
        f_wrapped = _wrap_pandas_func(f, preserve_index) if self._pandas_mode else f
        rdd = self._rdd.map(f_wrapped, preservesPartitioning=preserves_partitioning)
        return self._wrap_rdd(rdd)

    @property
    def _rdd(self):
        return self._rdd_obj

    def toDF(self):
        schema = self._rdd.map(lambda batch: from_arrow_schema(batch.schema)).first()

        def stream_to_batches(batch_iter):
            for batch in batch_iter:
                buf = batch.serialize()
                sink = pa.BufferOutputStream()
                sink.write(buf)
                # TODO: verify padded correctly
                pb = sink.getvalue().to_pybytes()
                yield pb

        self._rdd._jrdd_deserializer = NoOpSerializer()
        rdd = self._rdd.mapPartitions(stream_to_batches)
        rdd._jrdd_deserializer = NoOpSerializer()
        jdf = self._sql_ctx._jvm.PythonSQLUtils.toDataFrame(
            rdd._jrdd, schema.json(), self._sql_ctx._jsqlContext)
        return DataFrame(jdf, self._sql_ctx)


class ArrowDataFrame(ArrowRDDBase):
    """
    Wraps a Python DataFrame  using ``pandas.DataFrame``.
    """

    def __init__(self, data_frame, pandas_mode):
        super(ArrowDataFrame, self).__init__(data_frame.sql_ctx, pandas_mode)
        self.df = data_frame
        self._lazy_rdd = None

    @property
    def _arrow_rdd(self):
        if self._lazy_rdd is None:
            batch_jrdd = self.df._jdf.toArrowBatchRdd().toJavaRDD()
            ser = ArrowBatchToStreamSerializer(self.df.schema)
            rdd = RDD(batch_jrdd, self._sql_ctx._sc, jrdd_deserializer=ser)
            self._lazy_rdd = ArrowRDD(rdd, self._sql_ctx, self._pandas_mode)
        return self._lazy_rdd

    @property
    def _rdd(self):
        return self._arrow_rdd._rdd

    def map(self, f, preserves_partitioning=False, preserve_index=False):

        f_wrapped = _wrap_pandas_func(f, preserve_index) if self._pandas_mode else f

        def process_batched_partitions(batch_iter):
            for batch in batch_iter:
                yield f_wrapped(batch)

        rdd = self._rdd.mapPartitions(process_batched_partitions,
                                      preservesPartitioning=preserves_partitioning)
        return self._wrap_rdd(rdd)


class ArrowFramedSerializer(FramedSerializer):

    def dumps(self, batch):
        import io
        sink = io.BytesIO()
        serializer = ArrowStreamSerializer()
        serializer.dump_stream([batch], sink)
        return sink.getvalue()

    def loads(self, obj):
        serializer = ArrowStreamSerializer()
        return list(serializer.load_stream(pa.BufferReader(obj)))[0]

    def __repr__(self):
        return "ArrowFramedSerializer"


class ArrowBatchToStreamSerializer(ArrowFramedSerializer):

    def __init__(self, schema):
        self.schema = schema

    def loads(self, obj):
        buf = pa.BufferReader(obj)
        batch = pa.read_record_batch(buf, to_arrow_schema(self.schema))
        return batch
