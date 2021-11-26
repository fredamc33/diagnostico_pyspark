from pyspark.sql import SparkSession
import os

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.engine.Transformer import Transformer

if __name__ == '__main__':
    spark: SparkSession = SparkSession \
        .builder \
        .master(SPARK_MODE) \
        .getOrCreate()
    transformer = Transformer(spark, PATH_CONGIF.replace("\\", os.sep))

    folderName = 'EXERCISE_5'
    title = '**************** ' + folderName + ' ****************'
    transformer.print_exercise_title(title)
    df = transformer.exercise_5()
    df.show(n=10, truncate=False)

    folderName = 'EXERCISE_1'
    title = '**************** ' + folderName + ' ****************'
    transformer.print_exercise_title(title)
    df = transformer.exercise_1(df)
    df.show(n=10, truncate=False)
    transformer.write(df, folderName)

    folderName = 'EXERCISE_2'
    title = '**************** ' + folderName + ' ****************'
    transformer.print_exercise_title(title)
    df = transformer.exercise_2(df)
    df.show(n=10, truncate=False)
    transformer.write(df, folderName)

    folderName = 'EXERCISE_3'
    title = '**************** ' + folderName + ' ****************'
    transformer.print_exercise_title(title)
    df = transformer.exercise_3(df)
    df.show(n=10, truncate=False)
    transformer.write(df, folderName)

    folderName = 'EXERCISE_4'
    title = '**************** ' + folderName + ' ****************'
    transformer.print_exercise_title(title)

    df1 = transformer.exercise_4(df, 'A,B')
    df1.show(n=10, truncate=False)
    transformer.write(df1, folderName + '_1')

    df2 = transformer.exercise_4(df, 'C', 1.15)
    df2.show(n=10, truncate=False)
    transformer.write(df2, folderName + '_2')

    df3 = transformer.exercise_4(df, 'D', 1.25)
    df3.show(n=10, truncate=False)
    transformer.write(df3, folderName + '_3')