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

from pyspark.rdd import RDD
from pyspark.sql import DataFrame, GroupedData
from pyspark.serializers import ArrowPandasSerializer

'''
class DataFrame(object):
    ...
    def asPandas(self):
        return ArrowDataFrame(self)

def mapPartitionsAsPandas(self, f):
 +        """
 +        Return an Arrow RDD with function applied to a ``pandas.DataFrame`` at each partition.
 +        """
 +        def f_process_pandas(pdf_iter):
 +            for pdf in pdf_iter:
 +                yield f(pdf)
 +
 +        payload_jrdd = self._jdf.toArrowPayloadBytes().toJavaRDD()
 +        rdd = ArrowRDD(payload_jrdd, self._sc)
 +        return rdd.mapPartitions(f_process_pandas)
'''

class ArrowRDDBase(object):
    """
    Wraps a Python RDD to deserialize using Arrow into ``pandas.DataFrame`` for processing.
    """

    def __init__(self, sql_ctx):
        self._sql_ctx = sql_ctx

    @property
    def _rdd(self):
        raise NotImplementedError

    def _wrap_rdd(self, rdd):
        rdd._jrdd_deserializer = self._rdd._jrdd_deserializer
        return ArrowRDD(jrdd=None, sql_ctx=self._sql_ctx, pipelined_rdd=rdd)

    def map(self, f, preservesPartitioning=False):
        rdd = self._rdd.map(f, preservesPartitioning=preservesPartitioning)
        return self._wrap_rdd(rdd)

    def reduce(self, f):
        return self._rdd.reduce(f)

    def count(self):
        return self._rdd.count()

    def collect(self):
        return self._rdd.collect()


class ArrowDataFrame(ArrowRDDBase):
    """
    Wraps a Python DataFrame to group/winow then apply using``pandas.DataFrame``
    """

    def __init__(self, data_frame):
        super(ArrowDataFrame, self).__init__(data_frame.sql_ctx)
        self.df = data_frame
        self._lazy_rdd = None

    @property
    def _rdd(self):
        if self._lazy_rdd is None:
            payload_jrdd = self.df._jdf.toArrowPayloadBytes().toJavaRDD()
            self._lazy_rdd = ArrowRDD(payload_jrdd, self._sql_ctx)
        return self._lazy_rdd

    def groupBy(self, *cols):
        jgd = self._jdf.groupBy(self._jcols(*cols))
        return ArrowGroupedData(jgd, self.df.sql_ctx)

    def windowOver(self, window_spec):
        raise NotImplementedError()


class ArrowGroupedData(GroupedData):
    """
    Wraps a Python GroupedData object to process groups as ``pandas.DataFrame``
    """

    def __init__(self, jgd, sql_ctx):
        super(ArrowGroupedData, self).__init__(jgd, sql_ctx)

    def agg(self, f):
        # Apply function f to each group
        return DataFrame(...)


class ArrowRDD(ArrowRDDBase):
    """
    Wraps a Python RDD to deserialize using Arrow into ``pandas.DataFrame`` for processing.
    """

    def __init__(self, jrdd, sql_ctx, pipelined_rdd=None):
        super(ArrowRDD, self).__init__(sql_ctx)
        if pipelined_rdd is None:
            self._rdd_obj = RDD(jrdd, self._sql_ctx._sc, jrdd_deserializer=ArrowPandasSerializer())
        else:
            self._rdd_obj = pipelined_rdd

    @property
    def _rdd(self):
        return self._rdd_obj

    def toDF(self):
        schema = _parse_datatype_json_string(jschema.json())
        return self._sql_ctx.sparkSession.createDataFrame(self._rdd, schema=schema)
