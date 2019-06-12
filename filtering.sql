#remove row which less than 5 row per cat value
WITH
count_group_by_ip_table AS (
  SELECT ip, COUNT(*) as ip_group_count
  FROM `talkingdata.train`
  GROUP BY ip
), 
count_group_by_app_table AS (
  SELECT app, COUNT(*) as app_group_count
  FROM `talkingdata.train`
  GROUP BY app
),
count_group_by_device_table AS (
  SELECT device, COUNT(*) as device_group_count
  FROM `talkingdata.train`
  GROUP BY device
),
count_group_by_os_table AS (
  SELECT os, COUNT(*) as os_group_count
  FROM `talkingdata.train`
  GROUP BY os
),
count_group_by_channel_table AS (
  SELECT channel, COUNT(*) as channel_group_count
  FROM `talkingdata.train`
  GROUP BY channel
)
SELECT
   GENERATE_UUID() as index, train.* FROM
   talkingdata.train as train, count_group_by_ip_table, count_group_by_app_table, count_group_by_device_table, count_group_by_os_table, count_group_by_channel_table
   WHERE train.ip = count_group_by_ip_table.ip AND count_group_by_ip_table.ip_group_count >= 5 AND
   train.app = count_group_by_app_table.app AND count_group_by_app_table.app_group_count >= 5 AND
   train.device = count_group_by_device_table.device AND count_group_by_device_table.device_group_count >= 5 AND
   train.os = count_group_by_os_table.os AND count_group_by_os_table.os_group_count >= 5 AND
   train.channel = count_group_by_channel_table.channel AND count_group_by_channel_table.channel_group_count >= 5    