import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf,SparkContext}

// Declare the time interval window in mins to identify a session
val sessionWindowMins = 15
val input_path = "hdfs://user/amruth.raju/test"
val output_path = input_path+"/output"

// Since the access logs has fixed format, we can use the below schema to parse it into dataframe
val schema = new StructType()
.add("timestamp",StringType)
.add("elb",StringType)
.add("client_ip",StringType)
.add("backend_ip",StringType)
.add("request_processing_time",DoubleType)
.add("backend_processing_time",DoubleType)
.add("response_processing_time",DoubleType)
.add("elb_status_code",StringType)
.add("backend_status_code",StringType)
.add("received_bytes",StringType)
.add("sent_bytes",StringType)
.add("request",StringType)
.add("user_agent",StringType)
.add("ssl_cipher",StringType)
.add("ssl_protocol",StringType)

// read the file as csv with space delimiter
val webLogDF = spark.read.schema(schema).option("delimiter"," ").csv(s"${input_path}/*.gz")
.withColumn("epoch_seconds",unix_timestamp(to_timestamp(trim($"timestamp"))))

// Observed that few backend ip values were not present, hence cleaning the data because a session is not complete if its not serviced by backend. Select only required columns
val webLogFilteredDF =  webLogDF.filter($"backend_ip" =!= "-").select("timestamp","client_ip","request","user_agent","epoch_seconds")
// Window funcion to partition by IP and user agent
// Bonus: As per the AWS field description user_agent specifies the client from where the request originated and since a session cannot be shared across browsers/client it can be used along with the IP to identifiy a unique user.
val winFunc = Window.partitionBy($"client_ip",$"user_agent").orderBy("epoch_seconds")

// 1) Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
val webLogDFSessionized = webLogFilteredDF
// using lag function to retreive the previous request time for the given user.
.withColumn("start_time",lag($"epoch_seconds",1).over(winFunc))
// clean null values for the session start time
.withColumn("start_time",coalesce($"start_time",$"epoch_seconds"))
// calculate the duration of each requests
.withColumn("request_duration_secs",$"epoch_seconds" - $"start_time")
// Check if the duration is more than 15 mins
.withColumn("is_unique_session",when($"request_duration_secs".gt(sessionWindowMins * 60),1).otherwise(0))
// mark each unique session for a given user with incremental number.
.withColumn("session_marker",sum("is_unique_session").over(winFunc))
// assign a session id to each unique session using the hash function
.withColumn("session_id",hash(concat($"client_ip",lit("@@"),$"user_agent",lit("@@"),$"session_marker")))
// ignore the start of the session in further calculations because its duration will be always greater than 15 mins
.withColumn("request_duration_secs",when($"is_unique_session" === 1,0).otherwise($"request_duration_secs"))
// caching the data
.cache

// 2) Determine the average session time.
val avgSessionTimeDF = webLogDFSessionized.groupBy("session_id").agg(sum("request_duration_secs").alias("session_time_secs")).select(avg("session_time_secs") as "avg_session_time_secs")

// 3) Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.")
val uniqueURLVisitsDF = webLogDFSessionized
// .withColumn("request", split($"request"," ")(1)) // enable this line only if the url part of request has to be considered
.dropDuplicates("session_id","request").groupBy("session_id").count

// 4) Find the most engaged users, ie the IPs with the longest session times.
val mostEngagedUsersDF = webLogDFSessionized.groupBy("client_ip","session_id").agg(sum("request_duration_secs").alias("session_time_millis")).orderBy($"session_time_millis".desc)

println(" (1) Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.")
// prints sample data in the console
webLogDFSessionized.show(truncate=false)
// write the dataframe as csv format in the output path
webLogDFSessionized.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip").csv(s"${output_path}/weblog_sessionized_data")
println(" (2) Determine the average session time.")
avgSessionTimeDF.show(truncate=false)
avgSessionTimeDF.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip").csv(s"${output_path}/average_session_time")
println(" (3) Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.")
uniqueURLVisitsDF.show(truncate=false)
uniqueURLVisitsDF.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip").csv(s"${output_path}/unique_url_visits")
println(" (4) Find the most engaged users, ie the IPs with the longest session times.")
mostEngagedUsersDF.show(truncate=false)
mostEngagedUsersDF.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip").csv(s"${output_path}/most_engaged_users")
