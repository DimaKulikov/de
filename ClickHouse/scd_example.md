# Вводные
Получаем каждые 2 часа csv файлы с данными о сущности "доступный ресурс". Бизнес ключ - AVAILABLE_RES_ID, остальные поля являются его атрибутами.
Входные файлы - полный снапшот всех данных. 
Необходимо выявить новые бизнес ключи и изменившиеся атрибуты, сохранить актуальную версию и историю изменений. 

Используем ClickHouse (версия 24.3 на тот момент). Кликхаус не поддерживает MERGE INTO, который упростил бы схему обновления таблицы, поэтому используем следующую схему. 
1. Создаём целевую таблицу для измерения
```sql
CREATE OR REPLACE TABLE CORE.AVAILABLE_RESOURCE_SUPP (
	MO_ID LowCardinality(String)
	,MO_SHORT LowCardinality(String)
	,FIL_ID LowCardinality(String)
	,FIL_SHORT LowCardinality(String)
	,AVAILABLE_RES_ID String
	,AVAILABLE_RES String
	,AVAILABLE_RES_CODE String
	,AVAILABLE_RES_SPECIALIZATION_ID LowCardinality(String) 
	,AVAILABLE_RES_SP_NAME LowCardinality(String)
	,IS_SELF_REGISTRATION LowCardinality(String)
	--,START_DATE
	,START_DATE Date
	--,AVAILABLE_RES_REMOVE_DATETIME 
	,AVAILABLE_RES_REMOVE_DATETIME Nullable(DateTime('Europe/Moscow'))  
	,IS_ARCHIVED LowCardinality(String)
	,AVAILABLE_RES_LAST_EDIT_DATETIME Nullable(DateTime('Europe/Moscow'))
	--,AVAILABLE_RES_LAST_EDIT_DATETIME
	,AVAILABLE_RES_SCHEDULE_SECTION_ID LowCardinality(String)
	,AVAILABLE_RES_TYPE_ID LowCardinality(String)
	,MEJI_ID String
	,DEPARTMENT String
	,IS_DUTY LowCardinality(String)
	,AVAILABLE_RES_SPECIALIZATION_CHANGE_ABILITY LowCardinality(String)
	,MO LowCardinality(String)
	,FIL LowCardinality(String)
	,ME_ID String
	,SPEC_ID LowCardinality(String)
	,SPEC LowCardinality(String)
	--,FILENAME
	,FINGERPRINT UInt64
	,VALID_FROM DateTime('Europe/Moscow')
	,VALID_TO DateTime('Europe/Moscow')
	,IS_CURRENT UInt8
)
ENGINE = MergeTree()
ORDER BY AVAILABLE_RES_ID
```

2. Создаём [параметризованное представление](https://clickhouse.com/docs/en/sql-reference/statements/create/view#parameterized-view), которое будет участвовать в etl процессе. Представление смотрит в слой сырых данных RAW.DWH_AVAILABLE_RESOURCE_SUPP, и возвращает строки с изменившимися атрибутами. Изменяемыми атрибутами считаем не все атрибуты, а только некоторые исходя их бизнес логики.
```sql
CREATE OR REPLACE VIEW CORE.AVAILABLE_RESOURCE_SUPP_UPDATES
AS
WITH PREVIOUS_FINGERPRINT AS (
	SELECT AVAILABLE_RES_ID, FINGERPRINT AS PREVIOUS_FINGERPRINT
	FROM CORE.AVAILABLE_RESOURCE_SUPP ars
	WHERE IS_CURRENT = 1
),
UPDATES AS (
	SELECT 
	MO_ID
	,MO_SHORT
	,FIL_ID 
	,FIL_SHORT
	,AVAILABLE_RES_ID
	,AVAILABLE_RES
	,AVAILABLE_RES_CODE 
	,AVAILABLE_RES_SPECIALIZATION_ID 
	,AVAILABLE_RES_SP_NAME 
	,IS_SELF_REGISTRATION 
	,toDate(parseDateTime(substringUTF8(AVAILABLE_RES_CODE,3,8),'%Y%m%d','Europe/Moscow')) START_DATE
	,CASE 
		WHEN match(AVAILABLE_RES_REMOVE_DATETIME,'^\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}')
		THEN parseDateTimeOrNull(substring(AVAILABLE_RES_REMOVE_DATETIME,1,17),'%d.%m.%y %H:%i:%S','Europe/Moscow')
		WHEN match(AVAILABLE_RES_REMOVE_DATETIME,'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}')
		THEN parseDateTimeBestEffortOrNull(AVAILABLE_RES_REMOVE_DATETIME,'Europe/Moscow')
	END AVAILABLE_RES_REMOVE_DATETIME
	,IS_ARCHIVED
	,CASE 
		WHEN match(AVAILABLE_RES_LAST_EDIT_DATETIME,'^\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}')
		THEN parseDateTimeOrNull(substring(AVAILABLE_RES_LAST_EDIT_DATETIME,1,17),'%d.%m.%y %H:%i:%S','Europe/Moscow')
		WHEN match(AVAILABLE_RES_LAST_EDIT_DATETIME,'^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}')
		THEN parseDateTimeBestEffortOrNull(AVAILABLE_RES_LAST_EDIT_DATETIME,'Europe/Moscow')
	END AVAILABLE_RES_LAST_EDIT_DATETIME 
	,AVAILABLE_RES_SCHEDULE_SECTION_ID
	,AVAILABLE_RES_TYPE_ID
	,MEJI_ID
	,DEPARTMENT 
	,IS_DUTY 
	,AVAILABLE_RES_SPECIALIZATION_CHANGE_ABILITY 
	,MO
	,FIL 
	,ME_ID
	,SPEC_ID 
	,SPEC 
	--,FILENAME
	,cityHash64(
		ifNull(AVAILABLE_RES_SPECIALIZATION_ID,''),
		ifNull(IS_SELF_REGISTRATION,''),
		ifNull(AVAILABLE_RES,''),
		ifNull(AVAILABLE_RES_REMOVE_DATETIME, toDateTime('1900-01-01', 'Europe/Moscow')),
		ifNull(IS_ARCHIVED,''),
		ifNull(AVAILABLE_RES_LAST_EDIT_DATETIME, toDateTime('1900-01-01', 'Europe/Moscow')),
		ifNull(AVAILABLE_RES_SCHEDULE_SECTION_ID,''),
		ifNull(AVAILABLE_RES_TYPE_ID,''),
		ifNull(MEJI_ID,''),
		ifNull(IS_DUTY,'')
	) FINGERPRINT
	,parseDateTime(toString(FILENAME),'%Y%m%d%H', 'Europe/Moscow') VALID_FROM
	,toDateTime('2900-01-01', 'Europe/Moscow') VALID_TO
	,PREVIOUS_FINGERPRINT
	,1 IS_CURRENT
	FROM RAW.DWH_AVAILABLE_RESOURCE_SUPP dars
	LEFT OUTER JOIN PREVIOUS_FINGERPRINT
	USING (AVAILABLE_RES_ID)
	WHERE FILENAME = {current_filename:UInt32}
	AND (
		FINGERPRINT <> PREVIOUS_FINGERPRINT
		OR PREVIOUS_FINGERPRINT IS NULL
		OR PREVIOUS_FINGERPRINT = 0
	)
)
SELECT * FROM UPDATES
```

3. Для обновления данных добавляем DAG в airflow, который ждёт новые файлы и выполняет следующие 2 запроса, каждый раз после получений нового файла с данными. Первый запрос для добавления новых и изменившихся строк. Файлы получаем каждые 2 часа, в запросе изменяем параметр current_filename.
```sql
INSERT INTO CORE.AVAILABLE_RESOURCE_SUPP
SELECT 
MO_ID
,MO_SHORT
,FIL_ID
,FIL_SHORT
,AVAILABLE_RES_ID
,AVAILABLE_RES
,AVAILABLE_RES_CODE
,AVAILABLE_RES_SPECIALIZATION_ID
,AVAILABLE_RES_SP_NAME
,IS_SELF_REGISTRATION
,START_DATE
,AVAILABLE_RES_REMOVE_DATETIME
,IS_ARCHIVED
,AVAILABLE_RES_LAST_EDIT_DATETIME
,AVAILABLE_RES_SCHEDULE_SECTION_ID
,AVAILABLE_RES_TYPE_ID
,MEJI_ID
,DEPARTMENT
,IS_DUTY
,AVAILABLE_RES_SPECIALIZATION_CHANGE_ABILITY
,MO
,FIL
,ME_ID
,SPEC_ID
,SPEC
,FINGERPRINT
,VALID_FROM
,VALID_TO
,IS_CURRENT
FROM CORE.AVAILABLE_RESOURCE_SUPP_UPDATES(current_filename=2022052510)
```

4. Второй запрос для обновления строк с данными, которые устарели. В запросе в двух местах подставляется дата файла в формате YYYYMMDDHH. Используем настройку mutations_sync = 1, чтобы процесс дожидался завершения обновления (мутации в ClickHouse) и таск в airflow не завершался до завершения обновления.
ALTER TABLE 
CORE.AVAILABLE_RESOURCE_SUPP 
UPDATE VALID_TO = parseDateTime(toString(2022052512),'%Y%m%d%H', 'Europe/Moscow') - INTERVAL 1 SECOND
,IS_CURRENT = 0
WHERE AVAILABLE_RES_ID || toString(FINGERPRINT) IN 
(
	SELECT AVAILABLE_RES_ID || toString(PREVIOUS_FINGERPRINT) FROM CORE.AVAILABLE_RESOURCE_SUPP_UPDATES(current_filename=2022052512)
)
SETTINGS
mutations_sync = 1
