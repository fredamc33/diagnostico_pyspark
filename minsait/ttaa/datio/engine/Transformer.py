import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column
import json

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer
from minsait.ttaa.datio.enum.Mode import *

class Transformer(Writer):
    def __init__(self, spark: SparkSession, configPath):
        self.spark: SparkSession = spark
        self.configPath = configPath
        #df: DataFrame = self.read_input()
        #df.printSchema()
        #df = self.clean_data(df)
        #df = self.example_window_function(df)
        #df = self.column_selection(df)

        # for show 100 records after your transformations and show the DataFrame schema
        #df.show(n=100, truncate=False)
        #df.printSchema()

        self.df: DataFrame = self.read_input()

        # Uncomment when you want write your final output
        #self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            overall.column(),
            height_cm.column(),
            team_position.column(),
            catHeightByPosition.column()
        )
        return df

    def example_window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(team_position.column()) \
            .orderBy(height_cm.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 10, "A") \
            .when(rank < 50, "B") \
            .otherwise("C")

        df = df.withColumn(catHeightByPosition.name, rule)
        return df

    def print_exercise_title(self, title):
        """
        :param title: exercise title
        :return: show exercise title
        """
        print('*' * len(title))
        print(title)
        print('*' * len(title))

    def read_config(self):
        """
        :return: execution mode (1 or 0)
        """

        config = open(self.configPath)
        dictConfigFile = json.load(config)

        return dictConfigFile[MODE]

    def exercise_1(self, df: DataFrame):
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 10 columns...
        """
        df = df.select(
            short_name.column()
            , long_name.column()
            , age.column()
            , height_cm.column()
            , weight_kg.column()
            , nationality.column()
            , club_name.column()
            , overall.column()
            , potential.column()
            , team_position.column()
        )

        return df

    def exercise_2(self, df: DataFrame):
        """
        :param df: is a DataFrame with players information
        :return: add to the DataFrame the column "player_cat"
             by each nationality and position value
             A if the player is one of the best 3 players in his position in his country.
             B if the player is one of the best 5 players in his position in his country.
             C if the player is one of the top 10 players at his position in his country.
             D for the rest of the players.
        """

        w: WindowSpec = Window \
            .partitionBy(nationality.column(), team_position.column()) \
            .orderBy(overall.column())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 3, "A") \
            .when(rank < 5, "B") \
            .when(rank < 10, "C") \
            .otherwise("D")

        df = df.withColumn(playerCat.name, rule)

        return df

    def exercise_3(self, df: DataFrame):
        """
        :param df: is a DataFrame with players information
        :return: add to the DataFrame the column "potential_vs_overall"
             by each row calculate Potential column divided by overall column
        """

        df = df.withColumn(potentialVSoverall.name, potential.column() / overall.column())

        return df

    def exercise_4(self, df: DataFrame, player_cat_values, potential_vs_overall_value=None):
        """
        :param df: is a DataFrame with players information
        :return: add to the DataFrame the column "potential_vs_overall"
             by each row calculate Potential column divided by overall column
        """
        valueList = player_cat_values.upper().split(',')

        df = df.filter(playerCat.column().isin(valueList))

        if(potential_vs_overall_value != None):
            df = df.filter(potentialVSoverall.column() > potential_vs_overall_value)

        return df

    def exercise_5(self):
        """
        :return: filtered dataframe, according to the execution mode:
             1 perform all steps only for players under 23 years of age
             0 to do it with all the players in the dataset
        """
        mode = self.read_config()
        df = self.df

        if(mode == Mode.ONE.value):
            df = self.df.filter(age.column() < 23)

        return df