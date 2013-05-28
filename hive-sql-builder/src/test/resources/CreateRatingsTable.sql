CREATE EXTERNAL TABLE ratings
(
  movie_id  INT,
  user_id   INT,
  rating    FLOAT,
  tstamp    BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '::'
STORED AS TEXTFILE
LOCATION '${project.build.directory}/ratings'