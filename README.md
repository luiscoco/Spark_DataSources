# Spark DataSources JDBC PostgreSQL

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

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/72893784-1f49-49e9-b7cc-ba3538631fb1)

## 1.4. Install winutils and set HADOOP_HOME environmental variable

We download or clone this git repository: https://github.com/kontext-tech/winutils

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/22cc3ecc-1847-4f8b-89d1-14a434982363)

We place the winutils folder in C:

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/c3d66801-374d-4d8b-a219-298b39b30141)

Set HADOOP_HOME environmental variable. There are two options:

a) Create a new variable HADOOP_HOME and the the following value

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/f9137714-0684-4a80-bbf7-284a553c17c8)

Then add the bin folder path to the PATH environmental variable

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/6cce020a-802c-4da5-8594-f2d00d163d5a)

b) Add the bin folder path to the PATH environmental variable.

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/743f6394-2eac-44a3-b51e-2fc7ab8ea632)

## 1.5. Install PostgreSQL and pgAdmin in you local laptop

How to Install PostgreSQL 15 on Windows 10 [ 2023 Update ] Complete guide | pgAdmin 4

https://www.youtube.com/watch?v=0n41UTkOBb0

In the following URL you can downlaod postgreSQL

https://www.postgresql.org/download/

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/8a71288b-abb3-406e-b871-1212e2944fe2)

In the following URL you can download pgAdmin

https://www.pgadmin.org/download/pgadmin-4-windows/

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/536bed55-792f-43ce-91a8-4fdeb100f981)

## 1.6. Run PostgreSQL in Docker container and create a database and populate a table

1. Download, install Docker Desktop

https://www.docker.com/products/docker-desktop/

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/58651335-5aa2-4a82-8c5a-5ed7b159b796)

Run Docker Desktop

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/7eeffeb3-a2c6-4d54-bb4b-c11c4fa0f02a)

2. Pull and run the PostgreSQL docker container.

For details see: https://hub.docker.com/_/postgres

Open command prompt as administrator and run the following command to run the PostgreSQL docker container.

```
docker run --name mypostgres -e POSTGRES_PASSWORD=password -p 5432:5432 -d postgres
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/ceac3c57-76ab-47a8-86c8-fb011decf94e)

This is a command to run a Docker container using the official PostgreSQL image from the Docker Hub. Let me break it down for you:

**docker run:** This is the command to run a Docker container.

**--name mypostgres:** This flag sets the name of the container to "mypostgres". You can use this name to refer to the container in other Docker commands.

**-e POSTGRES_PASSWORD=password:** This sets an environment variable within the container. In this case, it's setting the password for the PostgreSQL user to "password". You can change "password" to whatever you prefer.

**-p 5432:5432:** This flag maps the container's port 5432 (PostgreSQL's default port) to the host machine's port 5432. This means you can connect to the PostgreSQL database on the host machine using port 5432.

**-d postgres:** This specifies the Docker image to use. In this case, it's using the official PostgreSQL image from Docker Hub.

So, in summary, this Docker command is creating and running a PostgreSQL container named "mypostgres" with a specified password,

mapping the container's PostgreSQL port to the host machine's port, and running it in the background (-d flag).

3. We check the postegreSQL docker container is runnng. We also copy the ContainerID to execute it later.

```
docker ps -a
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/8a93ba36-22d5-4fef-90de-e6dbf063fd2d)

4. We execute the postgreSQL container.

```
docker start dockerContainerID
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/bf09a7a6-cf63-4677-b5a2-3bde716efd80)

We enter in the command bash in the running PostgreSQL docker container

```
docker exec -it dockerContainerID bash
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/e1fc4697-ddd2-4590-b19e-a3162ef30bd9)

5. We run this command

```
psql -U postgres -W
```

This is a command-line instruction for interacting with PostgreSQL, a popular open-source relational database management system. Let's break it down:

**psql**: This is the command-line client for PostgreSQL. It allows you to interact with the database using SQL queries and commands.

**-U postgres**: This specifies the username to connect to the database. In this case, it's set to "postgres." You're connecting as the user "postgres."

**-W**: This option prompts for the password. After entering the command, you'll be asked to enter the password for the specified user ("postgres" in this case). 

It's a security measure to ensure that only authorized users can access the database.

So, when you run this command, it initiates a connection to a PostgreSQL database as the user "postgres" and prompts you for the password before allowing access.

6. In Password enter the password we set when running the docker container

```
Password: password
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/b5442bf7-be60-42c7-8a60-d4d9405a42f1)

7. We create a new database called mydb

```
create database mydb;
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/26792731-e9e2-4b70-a9b8-ad7732a6268f)

8. For listing all the databases

```
\l
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/e8850324-f1d4-4a56-9bd0-272ae5f6bfc0)

9. Now we create a new table called t1 inside the mydb database

```
create table t1(id int);
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/4b76f0af-920a-4e58-8966-f9d309a3d186)

10. We select all rows and we check there is still no rows in the table

```
select * from t1;
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/3c6619cc-e5a1-48de-a9ac-c5f4157494f3)

11. We insert a row in the table

```
insert into t1 values(1);
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/e0277a17-abce-4048-9468-2bf1c830ac20)

12. Again we run the select to see the rows items

```
select * from t1;
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/713f2d5c-120d-4e0d-a58e-6d8ea4b67fb1)

13. We create a new user "myuser" and set the password "mypass" for that user

```
create user myuser with encrypted password 'mypass';
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/00d3da0a-d199-4eba-b5b2-85d25f47ffae)

14. We grant all privileges to the user for using the mydb database

```
grant all privileges on database mydb to myuser;
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/67f209d8-b2c1-4d07-b528-c29f2a855628)

15. We exit. Now we are in the root user

```
exit
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/776501fd-122c-4e25-94e0-28dbdd8945c1)

16. We clear the screen

```
clear
```

17. Connect to the database with the superuser

```
psql -U postgres -h localhost -p 5432 -d mydb
```
Try to create a table and insert a row running these commands:

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/4022506c-2a86-426c-847f-2b7e85740f92)

18. If you cannot create the table then follow these steps:

Grant necessary privileges to the myuser on the public schema

```
GRANT USAGE, CREATE ON SCHEMA public TO myuser;
```

Connect as myuser

```
psql -U myuser -h localhost -p 5432 -d mydb
```

19. Now try creating the table again

```
create table t1(id int);
```

We insert a row in the table

```
insert into t1 values(1);
```

20. Now we check with pgAdmin 4 that the database mydb and the table exist with values

First we have to start the PostgreSQL server. Run this command to start the server:

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl start -D "C:\Program Files\PostgreSQL\15\data" -o "-p 5433"
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/012a732f-d510-4490-b627-4e02ae527a39)

If you need to stop the server, you can use the following command:

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl stop -D "C:\Program Files\PostgreSQL\15\data"
```

If you need to know the status

```
C:\Program Files\PostgreSQL\15\bin>pg_ctl status -D "C:\Program Files\PostgreSQL\15\data"
```

21. Now we run the application "pg Admin 4"

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/39b221e8-4252-45ec-957d-34d7100d4527)

22. We right click on "Servers" and select the menu option "Register->Server..."

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/8f847ed1-4c00-413a-8dfb-6e6d1dbbfd1f)

23. Set the server connection values

We enter the server name "localhost", the server port "5432", the database name "mydb", the username "myuser" and the user password "mypass".

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/ac13fa61-a367-4be7-bbfc-f9e9d6760621)

We also have to input the connection name "mypostgres"

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/6d8ade26-5609-4888-862e-d2fb9bb977ba)

If you expand the tree you can see the database "mydb" and the table "t1"

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/f7a7b1bd-9d39-4554-89ee-6ee75c288eec)

You can run the select statement to see the first table rows

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/739e9056-44ee-4a5c-8ac0-cd97e42d991d)

25. If any problem accessing to table t1 from PostgreSQL then:

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/737d0bab-4eb0-4fe4-836d-1fca81e9b552)

It seems like the user myuser might not have the necessary privileges on the table t1. 

Let's make sure myuser has the right permissions:

Connect to the database as the superuser:

```
psql -U postgres -h localhost -p 5432 -d mydb
```

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/f1a6c971-61a7-48e3-b03b-adace5b9ea18)

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

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/d2133640-fc7f-4bbb-9970-eb2b63c8ec4c)

22. Now connect to the PostgreSQL database and table with pgAdmin 4

We 

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/08aae750-68d3-40f1-b611-3a74339302bb)

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/2f0aca3b-8c25-42b1-9899-824dc4a8b8dc)

## 1.7. Install PostgreSQL JDBC driver in Spark folder

Download the PostgreSQL JDBC driver from internet

https://jdbc.postgresql.org/download/

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/25f61e40-e8d2-499e-8a24-5efdb74272a0)

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/5641ba9a-6cec-4278-a4cf-7847a51acff7)

Place the jar file "postgresql-42.6.0.jar" inside the path spark jars folder: C:\spark-3.5.0-bin-hadoop3\jars

![image](https://github.com/luiscoco/Spark_DataSources/assets/32194879/295ab950-f2f5-4cf3-96cc-5cc323e698b2)

# 2. Run the application in IngelliJ

## 2.1. Prerequisites

Before running IntelliJ we have to check the Java and Spark installations.

To see the Java version open a command prompt and run the command:

```
java -version
```

Then we also run the command:

```
spark-shell
```

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/fad2ab07-fc98-4913-9a29-77f836892330)

We copy the Java, Spark and Scala versions in order to use this data later when creating out new Spark Scala project in IntelliJ.

Java version: 11

Spark version: 3.5.0

Scala version: 2.12.18

## 2.2. Run IngelliJ Community IDE.

Install the "Scala" plugin

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/b972e53d-958a-42e8-a297-061fd06353c4)

Create a new project

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/d732beb5-e55b-431c-a658-46519560bea4)

Ente the new project input data

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/0cdab561-d45e-4245-97df-5cdd13401276)

Then we can see the new project in IntelliJ

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/2df99f59-ea8b-43b2-b8c2-fcc8fbe5a372)

## 2.3. We input the bild.sbt file source code

```
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_JDBC_PostgreSQL"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"

libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"
```

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/6b7a09b1-1626-4e47-8644-0e33de7606c5)

Then we press the sbt button in the righ hand side menu and the reload project button

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/0b0af871-2095-4747-aa4a-e5f572e817dc)

After reloading the project dependencies we will see this result

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/0e010607-1fa2-404d-ace8-61c88c8910ba)

**IMPORTANT NOTE:** How to look for libraries dependencies in internet 

In **Maven repository** we can find the dependencies in internet. For example if we are looking for the "org.postgresql"   

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/971b2004-5e28-4ab1-9740-7a4676360c38)

We press in the version link, in this case we press in the 42.6.0 link

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/d5125f8e-7319-4c7e-931a-ec53e0dac8ec)

And then we select the SBT tab an copy the dependency reference in our build.sbt file

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/a2b32f81-6481-457b-885c-22e70c597d18)

Then we go to the build.sbt file and we copy the library dependency code:

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/94dd96dd-d4c9-4fdb-a668-981e4a848060)

## 2.4. How to create a new package and new file inside the package

Firs we create the package. For that we right click on the scala folder and we select the menu options New->Package and we input the new package name

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/4369dc37-7041-4746-8333-0af217aba5f3)

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/03a390e5-df2a-4319-8bb3-21304b021ce1)

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/89cfe31e-d174-4035-b871-fe4a6c6ec2f9)

Now we create a new file inside the package. We select the menu options "New->Scala Class" and the "Object"

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/b4900b59-db07-4aec-8670-d788e8f94560)

We input the new "Object" name and we set as "DataSource", an arbitrary name we invented

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/ce71f7ef-ef85-4324-8baf-90e898b8ccbc)

Now you can see the new package and inside the new file "DataSource.scala" with the new object "DataSource" 

![image](https://github.com/luiscoco/Spark_DataSources_JDBC_PostgreSQL/assets/32194879/29208814-ef64-4331-850e-ded374fdd49c)


## 2.5. See the scala source code: scala\com.sparkExample1\DataSources.scala file

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
