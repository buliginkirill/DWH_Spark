## Запуск

1. Загрузить репозиторий.
2. Запустить sudo docker compose -f compose.yaml up
3. Найти в логах строку вида "Running on http://172.19.0.5:5000"
4. Перейти по ссылке
5. Зайти по очереди на для выполнения операций (интерфейс не интерактивный, ход выполнения можно посмотреть в дампе docker compose) 

Действия:
1. Загрузить исходные данные -- загружает DDL и CSV в БД.
2. Удалить данные DWH (опционально) -- очищает таблицы dwh.*, нужно для отладки
3. Загрузить данные DWH -- загружает изменения, 
4. Прочитать витрину