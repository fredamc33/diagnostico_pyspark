from pyspark.sql import DataFrame
import os

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *


class Writer:
    def write(self, df: DataFrame):
        df \
            .coalesce(2) \
            .write \
            .partitionBy(team_position.name) \
            .mode(OVERWRITE) \
            .parquet(OUTPUT_PATH);

    def write(self, df: DataFrame, outputPath):
        df \
            .coalesce(1) \
            .write \
            .partitionBy(nationality.name) \
            .mode(OVERWRITE) \
            .parquet(OUTPUT_PATH + os.sep + outputPath);