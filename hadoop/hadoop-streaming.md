# Суть
Задача посчитать средннее значение чаевых, оставленных в Нью-Йоркском такси в 2020 году по месяцам и видам оплаты.
Исключительно ради ~~того чтобы прочувствовать боль прошлых поколений~~ практики решаю с помощью классической обработки map-reduce. Запускается притоновский скрипт через hadoop-streaming.jar
# Исходные данные
Исходные данные лежат в csv, скачал с публичного источника, загрузил их в s3 в yandex cloud.
# mapper
маппер должен вернуть ключи "год-месяц-тип оплаты" и значения чаевых. Беру только валидные данные: не пустой тип оплаты, не отрицательное значение чаевых, только 2020 год. Фильтрация и проверка валидности данных происходит с помощью try/except, результат отправляется в стандартный вывод с помощью print. Для составного ключа использую условные разделитель тильду.
```python
#! /opt/conda/bin/python
import sys
import csv
import datetime

def main():
    mapping = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    reader = csv.DictReader(sys.stdin, delimiter=',', fieldnames=[
        'VendorID',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'RatecodeID',
        'store_and_fwd_flag',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge',
    ])
    for line in reader:
        # try/except to ignore invalid values
        try:
            pickup_dt = datetime.datetime.strptime(line['tpep_pickup_datetime'], '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue
        if not (pickup_dt.year == 2020):
            continue
        try:
            method = mapping[int(line['payment_type'])]
        except (KeyError, ValueError):
            continue
        try:
            tip = float(line['tip_amount'])
        except ValueError:
            continue
        if tip < 0:
            continue
        print(f"{pickup_dt.strftime('%Y-%m')}~{method}\t{tip}")
if __name__ == '__main__':
    main()
```
# reducer
Редьюсер должен посчитать среднее значение для каждого ключа. Опираемся на тот факт, что на редьюсер данные приходят отсортированные по ключу. На выходе всей задачи хочу получить один файл, поэтому использую один редьюсер. Итерируемся по строкам стандартного ввода, считаем кол-во значение, считаем сумму, следим за изменением ключа.
```python
#! /opt/conda/bin/python
import sys

def main():
    key = None
    amount = 0
    current_key = None
    current_count = 0
    current_sum = 0
    _stdin = sys.stdin
    print('Month,Payment type,Tips average amount')
    for line in _stdin:
        line = line.strip()
        key, amount = line.split('\t', 1)
        if current_key == key:
            current_count += 1
            current_sum += float(amount)
        else:
            if current_count:
                print(*current_key.split('~', 1), current_sum / current_count, sep=',')
            current_key = key
            current_count = 1
            current_sum = float(amount)

    if current_key and current_key == key:
        print(*current_key.split('~', 1), current_sum / current_count, sep=',')

if __name__ == '__main__':
    main()
```
# запуск
Запускаю на неймноде кластера хадуп, в агрументах указываю 1 редьюсер, креды для s3
```bash
#!/bin/bash
MR_OUTPUT=/user/ubuntu/output-practice

hadoop fs -rm -r $MR_OUTPUT

hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-D mapreduce.job.name='taxi average tip' \
-D mapreduce.job.reduces=1 \
-D mapred.textoutputformat.separator='' \
-Dfs.s3a.access.key="" \
-Dfs.s3a.secret.key="" \
-input s3a://taxi-2020/l6 \
-output $MR_OUTPUT \
-file /home/ubuntu/l6-practice/mapper.py \
-mapper /home/ubuntu/l6-practice/mapper.py \
-file /home/ubuntu/l6-practice/reducer.py \
-reducer /home/ubuntu/l6-practice/reducer.py
```
