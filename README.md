# ДЗ4. Отказоустойчивость с Apache ZooKeeper

Ансамбль из трёх серверов ZooKeeper 3.9.6 в Docker с Java 17.
У каждого сервера свой ID и отдельные тома данных.

## Запуск

```bash
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose exec -T zk1 zkServer.sh status
docker compose exec -T zk2 zkServer.sh status
docker compose exec -T zk3 zkServer.sh status
```

Подключение клиента с компьютера: `127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183`.
