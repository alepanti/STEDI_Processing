CREATE EXTERNAL TABLE `customer_landing`(
	`customername` string,
	`email` string,
	`phone` string,
	`birthday` date,
	`serialnumber` string,
	`registrationdate` bigint,
	`lastupdatedate` bigint,
	`sharewithresearchasofdate` bigint,
	`sharewithpublicasofdate` bigint,
	`sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://d609-udacity/customer/landing/'
TBLPROPERTIES ('classification' = 'json')