# дока
https://cwiki.apache.org/confluence/display/Hive/LanguageManual

# простая практика
Тот же подсчёт среднего значения чаевых в такси, который делал на класическом map-reduce через [hadoop-streaming и python](./hadoop-streaming.md), только теперь на hive, что позволяет решить задачу напного проще и эффективнее. Данные также считываются из s3 бакета
1. создаю внешнюю таблицу которая смотрит в csv в s3. описание данных отсюда - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
2. чтобы заголовки не читались как строки у таблицы задаю свойство "skip.header.line.count"="1". долго искал, жаль что это не задокументировано https://issues.apache.org/jira/browse/HIVE-5795
```sql
create external table yellow_taxi.taxi_data (
	vendor_id tinyint,
	tpep_pickup_datetime timestamp,
	tpep_dropoff_datetime timestamp,
	Passenger_count smallint,
	trip_distance int,
	rate_code_id tinyint,
	store_and_fwd_flag char(1),
	pu_location_id int,
	do_location_id int,		
	payment_type tinyint,
	fare_amount decimal(6,2),
	extra decimal(6,2),
	mta_tax decimal(6,2),
	tip_amount decimal(6,2),	
	tolls_amount decimal(6,2),
	improvement_surcharge decimal(6,2),
	total_amount decimal(6,2),
	congestion_surcharge decimal(6,2),
	airport_fee decimal(6,2)	
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 's3a://taxi-2020/l6/'
tblproperties ("skip.header.line.count"="1")
```
3. группирую простым sql
```sql
with mapped_filtered as (
	select 
	date_format(tpep_pickup_datetime,'y-MM') as year_month,
	case 
		when payment_type = 1 then 'Credit card'
		when payment_type = 2 then 'Cash'
		when payment_type = 3 then 'No charge'
		when payment_type = 4 then 'Dispute'
		when payment_type = 5 then 'Unknown'
		when payment_type = 6 then 'Voided trip'
	end as payment_type,
	tip_amount
	from taxi_data td
	where date_format(tpep_pickup_datetime,'y') = 2020
	and payment_type is not null
)
select year_month, payment_type, avg(tip_amount) 
from mapped_filtered
group by year_month, payment_type
order by year_month, payment_type;
```
