CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `birthday` string COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` bigint COMMENT 'from deserializer', 
  `lastupdatedate` bigint COMMENT 'from deserializer', 
  `sharewithresearchasofdate` bigint COMMENT 'from deserializer', 
  `sharewithpublicasofdate` bigint COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithFriendsAsOfDate,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://projects-0a2497c6-0bf0-44e1-b6f6-44c78604c8bc/customer/landing/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='97143ae4-b9c0-4c01-9d26-6c7756215e47', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='step_trainer_customer_landing', 
  'averageRecordSize'='307', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='1', 
  'recordCount'='956', 
  'sizeKey'='293777', 
  'typeOfData'='file')