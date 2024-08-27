# yc cli
[документация](https://yandex.cloud/ru/docs/cli/quickstart)


# s3 object storage
## aws-cli
создать профиль или изменить дефолтный в ~/.aws/config или командой
aws configure
```
[default]
region = ru-central1
endpoint_url = https://storage.yandexcloud.net/
output = json
```
Не во всех версиях aws-cli поддерживается endpoint_url из профиля. Можно указывать каждый раз агрументом, или добавить алиас
aws s3 --endpoint-url=https://storage.yandexcloud.net ...
## профили
aws --profile=default

~/.aws/config - тут настройки

~/.aws/credentials - тут ключи и токены
## создание бакета
aws s3 mb s3://taxi-2020
## копирование файлов
aws s3 cp --recursive ./taxi/2020/ s3://taxi-2020
