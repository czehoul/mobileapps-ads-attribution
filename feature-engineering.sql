WITH cumcount_by_ip_app_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_app_past_5min,
	COUNT(*) over (PARTITION BY ip, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_app_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_app_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
),
cumcount_by_ip_app_os_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip, app, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_app_os_past_5min,
	COUNT(*) over (PARTITION BY ip, app, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_app_os_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip, app, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_app_os_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
),
cumcount_by_ip_device_os_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip, device, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_device_os_past_5min,
	COUNT(*) over (PARTITION BY ip, device, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_device_os_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip, device, os ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_device_os_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
),
cumcount_by_ip_device_os_app_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip, device, os, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_device_os_app_past_5min,
	COUNT(*) over (PARTITION BY ip, device, os, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_device_os_app_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip, device, os, app ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_device_os_app_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
),
cumcount_by_ip_device_os_app_channel_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip, device, os, app, channel ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_device_os_app_channel_past_5min,
	COUNT(*) over (PARTITION BY ip, device, os, app, channel ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_device_os_app_channel_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip, device, os, app, channel ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_device_os_app_channel_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
),
cumcount_unique_channel_by_ip_table AS (
	SELECT index, (SELECT COUNT(DISTINCT channel_arr) from unnest(duplicate_channel_arr) as channel_arr) as unique_channel_by_ip_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(channel) over (partition by ip order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_channel_arr from talkingdata.train_filtered)
),
cumcount_unique_app_by_ip_device_os_table AS (
	SELECT index, (SELECT COUNT(DISTINCT app_arr) from unnest(duplicate_app_arr) as app_arr) as unique_app_by_ip_device_os_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(app) over (partition by ip, device,os order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_app_arr from talkingdata.train_filtered)
),
cumcount_unique_app_by_ip_table AS (
	SELECT index, (SELECT COUNT(DISTINCT app_arr) from unnest(duplicate_app_arr) as app_arr) as unique_app_by_ip_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(app) over (partition by ip order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_app_arr from talkingdata.train_filtered)
),
cumcount_unique_channel_by_ip_device_os_table AS (
	SELECT index, (SELECT COUNT(DISTINCT channel_arr) from unnest(duplicate_channel_arr) as channel_arr) as unique_channel_by_ip_device_os_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(channel) over (partition by ip, device,os  order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_channel_arr from talkingdata.train_filtered)
),
cumcount_unique_os_by_ip_app_table AS (
	SELECT index, (SELECT COUNT(DISTINCT os_arr) from unnest(duplicate_os_arr) as os_arr) as unique_os_by_ip_app_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(os) over (partition by ip, app order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_os_arr from talkingdata.train_filtered)
),
cumcount_unique_device_by_ip_table AS (
	SELECT index, (SELECT COUNT(DISTINCT device_arr) from unnest(duplicate_device_arr) as device_arr) as unique_device_by_ip_cum_count 
	FROM (
	SELECT index, ARRAY_AGG(device) over (partition by ip order by TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND CURRENT ROW) as duplicate_device_arr from talkingdata.train_filtered)
),
cumcount_by_ip_table AS (
	SELECT index, COUNT(*) over (PARTITION BY ip ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 300 PRECEDING AND CURRENT ROW) as cumcount_by_ip_past_5min,
	COUNT(*) over (PARTITION BY ip ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 3600  PRECEDING AND 301 PRECEDING) as cumcount_by_ip_past_5min_to_1hr,
    COUNT(*) over (PARTITION BY ip ORDER BY TIMESTAMP_DIFF(click_time, TIMESTAMP("1970-01-01", "UTC"), SECOND) RANGE BETWEEN 28800  PRECEDING AND 3601 PRECEDING) as cumcount_by_ip_past_1hr_to_8hr 	
	FROM talkingdata.train_filtered
)

SELECT
  `talkingdata.train_filtered`.ip, `talkingdata.train_filtered`.app, `talkingdata.train_filtered`.device, `talkingdata.train_filtered`.os, `talkingdata.train_filtered`.channel, `talkingdata.train_filtered`.click_time, `talkingdata.train_filtered`.is_attributed, cumcount_by_ip_app_past_5min, cumcount_by_ip_app_past_5min_to_1hr, cumcount_by_ip_app_past_1hr_to_8hr, cumcount_by_ip_app_os_past_5min, cumcount_by_ip_app_os_past_5min_to_1hr, cumcount_by_ip_app_os_past_1hr_to_8hr, cumcount_by_ip_device_os_past_5min, cumcount_by_ip_device_os_past_5min_to_1hr, cumcount_by_ip_device_os_past_1hr_to_8hr, cumcount_by_ip_device_os_app_past_5min, cumcount_by_ip_device_os_app_past_5min_to_1hr, cumcount_by_ip_device_os_app_past_1hr_to_8hr, cumcount_by_ip_device_os_app_channel_past_5min, cumcount_by_ip_device_os_app_channel_past_5min_to_1hr, cumcount_by_ip_device_os_app_channel_past_1hr_to_8hr, unique_channel_by_ip_cum_count, unique_app_by_ip_device_os_cum_count, unique_app_by_ip_cum_count, unique_channel_by_app_cum_count, unique_channel_by_ip_device_os_cum_count, unique_os_by_ip_app_cum_count, unique_device_by_ip_cum_count, cumcount_by_ip_past_5min,
cumcount_by_ip_past_5min_to_1hr, cumcount_by_ip_past_1hr_to_8hr
FROM `talkingdata.train_filtered`, cumcount_by_ip_app_table, cumcount_by_ip_app_os_table, cumcount_by_ip_device_os_table, cumcount_by_ip_device_os_app_table, cumcount_by_ip_device_os_app_channel_table, cumcount_unique_channel_by_ip_table, cumcount_unique_app_by_ip_device_os_table, cumcount_unique_app_by_ip_table, cumcount_unique_channel_by_app_table, cumcount_unique_channel_by_ip_device_os_table, cumcount_unique_os_by_ip_app_table, cumcount_unique_device_by_ip_table, cumcount_by_ip_table
WHERE  
  `talkingdata.train_filtered`.index = cumcount_by_ip_app_table.index
  AND `talkingdata.train_filtered`.index = cumcount_by_ip_app_os_table.index
  AND `talkingdata.train_filtered`.index = cumcount_by_ip_device_os_table.index
  AND `talkingdata.train_filtered`.index = cumcount_by_ip_device_os_app_table.index
  AND `talkingdata.train_filtered`.index = cumcount_by_ip_device_os_app_channel_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_channel_by_ip_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_app_by_ip_device_os_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_app_by_ip_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_channel_by_app_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_channel_by_ip_device_os_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_os_by_ip_app_table.index
  AND `talkingdata.train_filtered`.index = cumcount_unique_device_by_ip_table.index
  AND `talkingdata.train_filtered`.index = cumcount_by_ip_table.index

