CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_db`.`step_trainer_landing`(
	`sensorReadingTime` bigint,
	`serialnumber` string,
	`distanceFromObject` integer
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://d609-udacity/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json')