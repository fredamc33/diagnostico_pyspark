import unittest

from pyspark.sql.functions import regexp_replace

from minsait.ttaa.datio.engine.Transformer import *
from minsait.ttaa.datio.common.Constants import *
from pyspark.sql import SparkSession
from minsait.ttaa.datio.common.naming.PlayerOutput import *

class TestTransformer(unittest.TestCase):
    def setUp(self):
        spark: SparkSession = SparkSession \
            .builder \
            .master(SPARK_MODE) \
            .getOrCreate()
        self.transformer = Transformer(spark, "global.params")
        self.df = self.transformer.exercise_5()
        self.df = self.transformer.exercise_1(self.df)
        self.df = self.transformer.exercise_2(self.df)
        self.df = self.transformer.exercise_3(self.df)

class TestInit(TestTransformer):
    def test_exercise_4_1(self):

        valueList = 'A,B'.upper()

        dfResult = self.transformer.exercise_4(self.df, valueList)
        totalResult = dfResult.count()

        dfTest = self.df.filter(playerCat.column().isin(valueList.split(',')))
        totalTest = dfTest.count()

        self.assertEqual(totalResult == totalTest, True)

    def test_exercise_4_2(self):

        valueList = 'C'
        potential_vs_overall_value = 1.15

        dfResult = self.transformer.exercise_4(self.df, valueList, potential_vs_overall_value)
        totalResult = dfResult.count()

        dfTest = self.df.filter(playerCat.column().isin(valueList.split(',')))
        dfTest = dfTest.filter(potentialVSoverall.column() > potential_vs_overall_value)
        totalTest = dfTest.count()

        self.assertEqual(totalResult == totalTest, True)

    def test_exercise_4_3(self):

        valueList = 'D'
        potential_vs_overall_value = 1.25

        dfResult = self.transformer.exercise_4(self.df, valueList, potential_vs_overall_value)
        totalResult = dfResult.count()

        dfTest = self.df.filter(playerCat.column().isin(valueList.split(',')))
        dfTest = dfTest.filter(potentialVSoverall.column() > potential_vs_overall_value)
        totalTest = dfTest.count()

        self.assertEqual(totalResult == totalTest, True)

