package it.polimi.mtds;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import it.polimi.mtds.utils.LogUtils;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Input: csv files containing sensor readings
 * schema ("location: String, dateTime: Timestamp, temperature: Float, humidity: Float)
 */
public class Stats {
    public static void main(String[] args) {
        LogUtils.setLogLevel();

        final String master = args.length > 0 ? args[0] : "local[4]";
        final String filePath = args.length > 1 ? args[1] : "./";
        final String appName = "it.polimi.mtds.Stats";

        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .getOrCreate();

        final List<StructField> mySchemaFields = new ArrayList<>();
        mySchemaFields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
        mySchemaFields.add(DataTypes.createStructField("dateTime", DataTypes.TimestampType, true));
        mySchemaFields.add(DataTypes.createStructField("temperature", DataTypes.FloatType, true));
        mySchemaFields.add(DataTypes.createStructField("humidity", DataTypes.FloatType, true));
        final StructType mySchema = DataTypes.createStructType(mySchemaFields);

        Dataset<Row> dataset = spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("dateFormat","dd/MM/yyyy HH:mm:ss")
                .schema(mySchema)
                //.csv(filePath + "../DataOut/DB.csv");
                .csv(filePath + "../DataOut/dataset.csv");

        dataset = dataset.withColumn("neighborhood",split(col("location"),"[.]").getItem(0))
                .withColumn("building",split(col("location"),"[.]").getItem(1))
                .withColumn("floor",split(col("location"),"[.]").getItem(2))
                .withColumn("room",split(col("location"),"[.]").getItem(3))
                .withColumn("fullFloorLocation", concat_ws(".", col("neighborhood"), col("building"), col("floor")))
                .withColumn("fullBuildingLocation", concat_ws(".", col("neighborhood"), col("building")))
                .withColumnRenamed("location", "fullRoomLocation")
                .withColumn("hour", hour(col("dateTime")))
                .withColumn("day", to_date(col("dateTime")))
                .withColumn("week", weekofyear(col("dateTime")))
                .withColumn("month", month(col("dateTime")))
                .withColumn("year", year(col("dateTime")))
                .withColumn("night", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(8).or(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(20)))
                .withColumn("daily", hour(col("dateTime")).cast(DataTypes.IntegerType).lt(20).and(hour(col("dateTime")).cast(DataTypes.IntegerType).geq(8)));
        dataset.show();
        dataset.cache();





        //Hourly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityHour = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityHour.cache();

        movingAverageTemperatureAndHumidityHour.createOrReplaceTempView("hourMovingAverage");

        Dataset<Row> movingAverageTemperatureAndHumidityHourRoom = spark.sql("SELECT fullRoomLocation, dateTime, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourRoom = movingAverageTemperatureAndHumidityHourRoom.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityHourRoom.show();
        writeToCsv(movingAverageTemperatureAndHumidityHourRoom, "movingAverageTemperatureAndHumidityHourRoom");

        //Hourly moving average - Building Level
        Dataset<Row> movingAverageTemperatureAndHumidityHourFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourFloor = movingAverageTemperatureAndHumidityHourFloor.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityHourFloor.show();
        writeToCsv(movingAverageTemperatureAndHumidityHourFloor, "movingAverageTemperatureAndHumidityHourFloor");

        //Hourly moving average - Building
        Dataset<Row> movingAverageTemperatureAndHumidityHourBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourBuilding = movingAverageTemperatureAndHumidityHourBuilding.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityHourBuilding.show();
        writeToCsv(movingAverageTemperatureAndHumidityHourBuilding, "movingAverageTemperatureAndHumidityHourBuilding");

        //Hourly moving average - Neighborhood-scale
        Dataset<Row> movingAverageTemperatureAndHumidityHourNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW) AS avg_hum FROM hourMovingAverage");
        movingAverageTemperatureAndHumidityHourNeighborhood = movingAverageTemperatureAndHumidityHourNeighborhood.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityHourNeighborhood.show();
        writeToCsv(movingAverageTemperatureAndHumidityHourNeighborhood, "movingAverageTemperatureAndHumidityHourNeighborhood");

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityHour.unpersist();



        //Daily moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityDaily = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityDaily.cache();
        movingAverageTemperatureAndHumidityDaily.createOrReplaceTempView("dailyMovingAverage");

        Dataset<Row> movingAverageTemperatureAndHumidityDailyRoom = spark.sql("SELECT fullRoomLocation, dateTime, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyRoom = movingAverageTemperatureAndHumidityDailyRoom.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityDailyRoom.show();
        writeToCsv(movingAverageTemperatureAndHumidityDailyRoom, "movingAverageTemperatureAndHumidityDailyRoom");

        //Daily moving average - Building Level
        Dataset<Row> movingAverageTemperatureAndHumidityDailyFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyFloor = movingAverageTemperatureAndHumidityDailyFloor.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityDailyFloor.show();
        writeToCsv(movingAverageTemperatureAndHumidityDailyFloor, "movingAverageTemperatureAndHumidityDailyFloor");

        //Daily moving average - Building
        Dataset<Row> movingAverageTemperatureAndHumidityDailyBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyBuilding = movingAverageTemperatureAndHumidityDailyBuilding.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityDailyBuilding.show();
        writeToCsv(movingAverageTemperatureAndHumidityDailyBuilding, "movingAverageTemperatureAndHumidityDailyBuilding");

        //Daily moving average - Neighborhood-scale
        Dataset<Row> movingAverageTemperatureAndHumidityDailyNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM dailyMovingAverage");
        movingAverageTemperatureAndHumidityDailyNeighborhood = movingAverageTemperatureAndHumidityDailyNeighborhood.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityDailyNeighborhood.show();
        writeToCsv(movingAverageTemperatureAndHumidityDailyNeighborhood, "movingAverageTemperatureAndHumidityDailyNeighborhood");

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityDaily.unpersist();



        //Weekly moving average - Room
        final Dataset<Row> movingAverageTemperatureAndHumidityWeekly = dataset
                .select(col("fullRoomLocation"), col("fullBuildingLocation"), col("fullFloorLocation"), col("neighborhood"), col("dateTime"), col("temperature"), col("humidity"));

        movingAverageTemperatureAndHumidityWeekly.cache();
        movingAverageTemperatureAndHumidityWeekly.createOrReplaceTempView("weeklyMovingAverage");

        Dataset<Row> movingAverageTemperatureAndHumidityWeeklyRoom = spark.sql("SELECT fullRoomLocation, dateTime, avg(temperature) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyRoom = movingAverageTemperatureAndHumidityWeeklyRoom.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityWeeklyRoom.show();
        writeToCsv(movingAverageTemperatureAndHumidityWeeklyRoom, "movingAverageTemperatureAndHumidityWeeklyRoom");

        //Weekly moving average - Building Level
        Dataset<Row> movingAverageTemperatureAndHumidityWeeklyFloor = spark.sql("SELECT fullFloorLocation, dateTime, avg(temperature) OVER (PARTITION BY fullFloorLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyFloor = movingAverageTemperatureAndHumidityWeeklyFloor.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityWeeklyFloor.show();
        writeToCsv(movingAverageTemperatureAndHumidityWeeklyFloor, "movingAverageTemperatureAndHumidityWeeklyFloor");

        //Weekly moving average - Building
        Dataset<Row> movingAverageTemperatureAndHumidityWeeklyBuilding = spark.sql("SELECT fullBuildingLocation, dateTime, avg(temperature) OVER (PARTITION BY fullBuildingLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyBuilding = movingAverageTemperatureAndHumidityWeeklyBuilding.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityWeeklyBuilding.show();
        writeToCsv(movingAverageTemperatureAndHumidityWeeklyBuilding, "movingAverageTemperatureAndHumidityWeeklyBuilding");

        //Weekly moving average - Neighborhood-scale
        Dataset<Row> movingAverageTemperatureAndHumidityWeeklyNeighborhood = spark.sql("SELECT neighborhood, dateTime, avg(temperature) OVER (PARTITION BY neighborhood ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_temp, avg(humidity) OVER (PARTITION BY fullRoomLocation ORDER BY CAST(dateTime AS timestamp) RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND CURRENT ROW) AS avg_hum FROM weeklyMovingAverage");
        movingAverageTemperatureAndHumidityWeeklyNeighborhood = movingAverageTemperatureAndHumidityWeeklyNeighborhood.withColumn("avg_temp", bround(col("avg_temp"), 2)).withColumn("avg_hum", bround(col("avg_hum"), 2));
        movingAverageTemperatureAndHumidityWeeklyNeighborhood.show();
        writeToCsv(movingAverageTemperatureAndHumidityWeeklyNeighborhood, "movingAverageTemperatureAndHumidityWeeklyNeighborhood");

        //No more used, so removed from cache
        movingAverageTemperatureAndHumidityWeekly.unpersist();





        //Daily night-day temperature difference - Room
        final Dataset<Row> meanDailyRoom = dataset
                .groupBy("daily", "day", "fullRoomLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyRoom.cache();

        final Dataset<Row> dailyRoom = meanDailyRoom
                .filter(col("daily").equalTo(true))
                .withColumnRenamed("avg_temp", "day_temp")
                .withColumnRenamed("avg_hum", "day_hum")
                .withColumn("day_temp", bround(col("day_temp"), 2))
                .withColumn("day_hum", bround(col("day_hum"), 2));
        final Dataset<Row> nightRoom = meanDailyRoom
                .filter(col("daily").equalTo(false))
                .withColumnRenamed("fullRoomLocation", "fl")
                .withColumnRenamed("day", "d")
                .withColumnRenamed("avg_temp", "night_temp")
                .withColumnRenamed("avg_hum", "night_hum")
                .withColumn("night_temp", bround(col("night_temp"), 2))
                .withColumn("night_hum", bround(col("night_hum"), 2));

        final Dataset<Row> diffRoom = dailyRoom.join(nightRoom, nightRoom.col("fl").equalTo(dailyRoom.col("fullRoomLocation")).and(nightRoom.col("d").equalTo(dailyRoom.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", bround(expr("day_temp-night_temp"), 2))
                .withColumn("hum_diff", bround(expr("day_hum-night_hum"), 2));
        diffRoom.cache();

        if (!diffRoom.isEmpty()){
            diffRoom.show();
            writeToCsv(diffRoom, "diffRoom");
        }



        //Daily night-day temperature difference - Floor Level
        final Dataset<Row> meanDailyFloor = dataset
                .groupBy("daily", "day", "fullFloorLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyFloor.cache();

        final Dataset<Row> dailyFloor = meanDailyFloor
                .filter(col("daily").equalTo(true))
                .withColumnRenamed("avg_temp", "day_temp")
                .withColumnRenamed("avg_hum", "day_hum")
                .withColumn("day_temp", bround(col("day_temp"), 2))
                .withColumn("day_hum", bround(col("day_hum"), 2));
        final Dataset<Row> nightFloor = meanDailyFloor
                .filter(col("daily").equalTo(false))
                .withColumnRenamed("fullFloorLocation", "fl")
                .withColumnRenamed("day", "d")
                .withColumnRenamed("avg_temp", "night_temp")
                .withColumnRenamed("avg_hum", "night_hum")
                .withColumn("night_temp", bround(col("night_temp"), 2))
                .withColumn("night_hum", bround(col("night_hum"), 2));

        final Dataset<Row> diffFloor = dailyFloor.join(nightFloor, nightFloor.col("fl").equalTo(dailyFloor.col("fullFloorLocation")).and(nightFloor.col("d").equalTo(dailyFloor.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", bround(expr("day_temp-night_temp"), 2))
                .withColumn("hum_diff", bround(expr("day_hum-night_hum"), 2));
        diffFloor.cache();

        if (!diffFloor.isEmpty()){
            diffFloor.show();
            writeToCsv(diffFloor, "diffFloor");
        }




        //Daily night-day temperature difference - Building Level
        final Dataset<Row> meanDailyBuilding = dataset
                .groupBy("daily", "day", "fullBuildingLocation")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyBuilding.cache();

        final Dataset<Row> dailyBuilding = meanDailyBuilding
                .filter(col("daily").equalTo(true))
                .withColumnRenamed("avg_temp", "day_temp")
                .withColumnRenamed("avg_hum", "day_hum")
                .withColumn("day_temp", bround(col("day_temp"), 2))
                .withColumn("day_hum", bround(col("day_hum"), 2));
        final Dataset<Row> nightBuilding = meanDailyBuilding
                .filter(col("daily").equalTo(false))
                .withColumnRenamed("fullBuildingLocation", "fl")
                .withColumnRenamed("day", "d")
                .withColumnRenamed("avg_temp", "night_temp")
                .withColumnRenamed("avg_hum", "night_hum")
                .withColumn("night_temp", bround(col("night_temp"), 2))
                .withColumn("night_hum", bround(col("night_hum"), 2));

        final Dataset<Row> diffBuilding = dailyBuilding.join(nightBuilding, nightBuilding.col("fl").equalTo(dailyBuilding.col("fullBuildingLocation")).and(nightBuilding.col("d").equalTo(dailyBuilding.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", bround(expr("day_temp-night_temp"), 2))
                .withColumn("hum_diff", bround(expr("day_hum-night_hum"), 2));
        diffBuilding.cache();

        if (!diffBuilding.isEmpty()) {
            diffBuilding.show();
            writeToCsv(diffBuilding, "diffBuilding");
        }



        //Daily night-day temperature difference - Neighborhood-scale
        final Dataset<Row> meanDailyNeighborhood = dataset
                .groupBy("daily", "day", "neighborhood")
                .agg(
                        avg("temperature").as("avg_temp"),
                        avg("humidity").as("avg_hum")
                )
                .orderBy("day");
        meanDailyNeighborhood.cache();

        final Dataset<Row> dailyNeighborhood = meanDailyNeighborhood
                .filter(col("daily").equalTo(true))
                .withColumnRenamed("avg_temp", "day_temp")
                .withColumnRenamed("avg_hum", "day_hum")
                .withColumn("day_temp", bround(col("day_temp"), 2))
                .withColumn("day_hum", bround(col("day_hum"), 2));
        final Dataset<Row> nightNeighborhood = meanDailyNeighborhood
                .filter(col("daily").equalTo(false))
                .withColumnRenamed("neighborhood", "fl")
                .withColumnRenamed("day", "d")
                .withColumnRenamed("avg_temp", "night_temp")
                .withColumnRenamed("avg_hum", "night_hum")
                .withColumn("night_temp", bround(col("night_temp"), 2))
                .withColumn("night_hum", bround(col("night_hum"), 2));

        final Dataset<Row> diffNeighborhood = dailyNeighborhood.join(nightNeighborhood, nightNeighborhood.col("fl").equalTo(dailyNeighborhood.col("neighborhood")).and(nightNeighborhood.col("d").equalTo(dailyNeighborhood.col("day"))), "left_outer")
                .filter(col("d").isNotNull())
                .drop("daily", "d", "fl")
                .withColumn("temp_diff", bround(expr("day_temp-night_temp"), 2))
                .withColumn("hum_diff", bround(expr("day_hum-night_hum"), 2));
        diffNeighborhood.cache();

        if (!diffNeighborhood.isEmpty()) {
            diffNeighborhood.show();
            writeToCsv(diffNeighborhood, "diffNeighborhood");
        }





        if (!diffRoom.isEmpty()) {
            //Month of the year with higher average night-day temperature difference - Room
            final Dataset<Row> avgMonthRoom = diffRoom.withColumn("month", month(col("day"))).groupBy("month")
                    .agg(
                            avg("temp_diff").as("temp"),
                            avg("hum_diff").as("hum")
                    )
                    .orderBy("month");
            avgMonthRoom.cache();
            //avgMonthRoom.show();

            final Dataset<Row> maxMonthRoom = avgMonthRoom
                    .select(col("month"), col("temp"))
                    .where(col("temp").equalTo(avgMonthRoom.select(max("temp")).first().getDouble(0)))
                    .withColumn("temp", bround(col("temp"), 2));
            maxMonthRoom.show();
            writeToCsv(maxMonthRoom, "maxMonthRoom");

            //No more used, so removed from cache
            maxMonthRoom.unpersist();
            avgMonthRoom.unpersist();
            diffRoom.unpersist();
            meanDailyRoom.unpersist();
        }



        if (!diffFloor.isEmpty()) {
            //Month of the year with higher average night-day temperature difference - Building Level
            final Dataset<Row> avgMonthFloor = diffFloor.withColumn("month", month(col("day"))).groupBy("month")
                    .agg(
                            avg("temp_diff").as("temp"),
                            avg("hum_diff").as("hum")
                    )
                    .orderBy("month");
            avgMonthFloor.cache();
            //avgMonthFloor.show();

            //No more used, so removed from cache
            diffFloor.unpersist();

            final Dataset<Row> maxMonthFloor = avgMonthFloor
                    .select(col("month"), col("temp"))
                    .where(col("temp").equalTo(avgMonthFloor.select(max("temp")).first().getDouble(0)))
                    .withColumn("temp", bround(col("temp"), 2));
            maxMonthFloor.show();
            writeToCsv(maxMonthFloor, "maxMonthFloor");

            //No more used, so removed from cache
            maxMonthFloor.unpersist();
            avgMonthFloor.unpersist();
            diffFloor.unpersist();
            meanDailyFloor.unpersist();
        }



        if (!diffBuilding.isEmpty()) {
            //Month of the year with higher average night-day temperature difference - Building
            final Dataset<Row> avgMonthBuilding = diffBuilding.withColumn("month", month(col("day"))).groupBy("month")
                    .agg(
                            avg("temp_diff").as("temp"),
                            avg("hum_diff").as("hum")
                    )
                    .orderBy("month");
            avgMonthBuilding.cache();
            //avgMonthBuilding.show();

            final Dataset<Row> maxMonthBuilding = avgMonthBuilding
                    .select(col("month"), col("temp"))
                    .where(col("temp").equalTo(avgMonthBuilding.select(max("temp")).first().getDouble(0)))
                    .withColumn("temp", bround(col("temp"), 2));
            maxMonthBuilding.show();
            writeToCsv(maxMonthBuilding, "maxMonthBuilding");

            //No more used, so removed from cache
            maxMonthBuilding.unpersist();
            avgMonthBuilding.unpersist();
            diffBuilding.unpersist();
            meanDailyBuilding.unpersist();
        }



        if (!diffNeighborhood.isEmpty()) {
            //Month of the year with higher average night-day temperature difference - Neighborhood-scale
            final Dataset<Row> avgMonthNeighborhood = diffNeighborhood.withColumn("month", month(col("day"))).groupBy("month")
                    .agg(
                            avg("temp_diff").as("temp"),
                            avg("hum_diff").as("hum")
                    )
                    .orderBy("month");
            avgMonthNeighborhood.cache();
            //avgMonthNeighborhood.show();

            final Dataset<Row> maxMonthNeighborhood = avgMonthNeighborhood
                    .select(col("month"), col("temp"))
                    .where(col("temp").equalTo(avgMonthNeighborhood.select(max("temp")).first().getDouble(0)))
                    .withColumn("temp", bround(col("temp"), 2));
            maxMonthNeighborhood.show();
            writeToCsv(maxMonthNeighborhood, "maxMonthNeighborhood");

            //No more used, so removed from cache
            maxMonthNeighborhood.unpersist();
            avgMonthNeighborhood.unpersist();
            diffNeighborhood.unpersist();
            meanDailyNeighborhood.unpersist();
        }

        dataset.unpersist();

        spark.close();

    }

    public static void writeToCsv(Dataset<Row> dataset, String path) {
        dataset
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ";")
                .csv("../Stats/" + path);
    }
}