# Spark DataSources

# 1. Previous steps: install IntelliJ Community + Scala plugin, Java 11, Spark and winutils in your computer

## 1.1. Install IntelliJ Community + Scala plugin

https://www.jetbrains.com/idea/download/?section=windows

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/d2cce9e7-6393-4a67-aef5-9a1b44d5c879)

## 1.2. Install Java 11 (JDK) and set JAVA_HOME environmental variable

https://www.oracle.com/es/java/technologies/javase/jdk11-archive-downloads.html

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/e7b24fb3-8de8-4b6a-9864-ea2eab2fec26)

## 1.3. Install Spark and set SPARK_HOME environmental variable or directly add the bin folder path in the PATH environmental variable

https://spark.apache.org/downloads.html

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/b07568f0-6399-48a9-ad31-e69d7cea4c92)

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/3e259f55-8881-4055-a38d-f8f6c8b954bf)

After unzipping the file "spark-3.5.0-bin-hadoop3" place the folder in your C: hard disk root.

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/a843fc85-8f75-43e3-a3ca-5f47ed7dc66e)

There are to optins for setting the Spark environmental variables:

a) Create a new variable SPARK_HOME and set the following value: C:\spark-3.5.0-bin-hadoop3

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/a08517be-e2c2-4fba-b830-0dec6c6f5ff0)

Then we set the bin folder path in the PATH environmental variable.

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/1adce829-e22b-4282-9e59-d6c85e034013)

b) Add the bin folder path to the PATH environmental variable.



## 1.4. Install winutils and set HADOOP_HOME environmental variable

https://github.com/kontext-tech/winutils


Set HADOOP_HOME environmental variable


Add the bin folder path to the PATH environmental variable


## 1.5. Install PostgreSQL and pgAdmin in you local laptop

How to Install PostgreSQL 15 on Windows 10 [ 2023 Update ] Complete guide | pgAdmin 4

https://www.youtube.com/watch?v=0n41UTkOBb0

In the following URL you can downlaod postgreSQL

https://www.postgresql.org/download/

In the following URL you can download pgAdmin

https://www.pgadmin.org/download/pgadmin-4-windows/

## 1.6. Run PostgreSQL in Docker container and create a database and populate a table

1. Pull and run the PostgreSQL docker container.

For details see: https://hub.docker.com/_/postgres

```
docker run --name mypostgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres
```


2. We check the postegreSQL docker container is running. We also copy the ContainerID to execute it later.

```
docker ps -a
```


3. We execute the postgreSQL container.

```
docker start dockerContainerID
```


```
docker exec -it dockerContainerID bash
```


4. We run this command

```
psql -U postgres -W
```

In Password enter the password we set when running the docker container

```
Password: password
```


6. We create a new database called mydb

```
create database mydb;
```


7. For listing all the databases

```
\l
```


8. Now we create a new table called t1 inside the mydb database

```
create table t1(id int);
```


9. We select all rows and we check there is still no rows in the table

```
select * from t1;
```

10. We insert a row in the table

```
insert into t1 values(1);
```

11. Again we run the select to see the rows items

```
select * from t1;
```

12. We create a new user and set the password for that user

```
create user myuser with encrypted password 'mypass';
```

13. We grant all privileges to the user for using the mydb database

```
grant all privileges on database mydb to myuser;
```

14. We exit

```
exit
```

15. Now we are in the root user

16. We clear the screen

```
clear
```

17. Connect to the database with the superuser

```
psql -U postgres -h localhost -p 5432 -d mydb
```

Grant necessary privileges to the myuser on the public schema

```
GRANT USAGE, CREATE ON SCHEMA public TO myuser;
```

Connect as myuser

```
psql -U myuser -h localhost -p 5432 -d mydb
```

18. Now try creating the table again

```
create table t1(id int);
```

19. Now we check with pgAdmin 4 that the database mydb and the table exist with values

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl start -D "C:\Program Files\PostgreSQL\15\data" -o "-p 5433"
```

If you need to stop the server, you can use the following command:

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl stop -D "C:\Program Files\PostgreSQL\15\data"
```

If you need to know the status

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl status -D "C:\Program Files\PostgreSQL\15\data"
```

19. If any problem accessing to table t1 from PostgreSQL then:

It seems like the user myuser might not have the necessary privileges on the table t1. 

Let's make sure myuser has the right permissions:

Connect to the database as the superuser:

```
psql -U postgres -h localhost -p 5432 -d mydb
```

Grant necessary privileges to myuser on the t1 table:

```
GRANT ALL PRIVILEGES ON TABLE t1 TO myuser;
```

Exit the PostgreSQL prompt:

```
\q
```

Now, connect to the database as myuser:

```
psql -U myuser -h localhost -p 5432 -d mydb
```

Try running the SELECT query again:

```
SELECT id FROM public.t1;
```

20. Now connect to the PostgreSQL database and table with pgAdmin 4


## 1.7. Install PostgreSQL JDBC driver in Spark folder

Download the PostgreSQL JDBC driver from internet



Unzip and place the jar file "postgresql-42.6.0.jar" inside the path spark jars folder: C:\spark-3.5.0-bin-hadoop3\jars




# 2. Run the application in IngelliJ

## 2.1. See the dependencies: build.sbt file

```
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "FirstSparkProject"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"
```

## 2.2. See the scala source code: scala\com.sparkExample1\DataSources.scala file

```scala
package com.sparkExample1

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "yyyy-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/mydb"
  val user = "myuser"
  val password = "mypass"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.t1")
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DF
/*
moviesDF.write
  .format("jdbc")
  .option("driver", driver)
  .option("url", url)
  .option("user", user)
  .option("password", password)
  .option("dbtable", "public.t1")
  .save()
  */
}
```
