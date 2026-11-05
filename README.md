# ДЗ4. Apache ZooKeeper

[Отчёт — ИнжДан_lab4_Вдовина А.Р.pdf](<ИнжДан_lab4_Вдовина А.Р.pdf>)

Три сервера ZooKeeper 3.9.6 в Docker и два координатора расчёта.

## Запуск

Нужны Docker Compose и Python 3.10+.

```bash
docker compose up -d --wait --wait-timeout 120
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
mkdir -p work
```

Положить полный `it_vacancies_2024.csv` в `work/`.
Ссылка на данные и SHA-256 — в [baseline/source.json](baseline/source.json).

## Проверки

```bash
.venv/bin/python scripts/ensemble_demo.py
.venv/bin/python scripts/failover_demo.py --mode both
.venv/bin/python -m pytest tests -q
```

Первый сценарий проверяет кворум и эфемерный узел, второй — смену
координатора без защиты и с защитой по эпохе. Журналы находятся в `logs/`,
результаты — в `results/`.

## Ручной запуск

```bash
FENCING=1 .venv/bin/python -m hw4.coordinator \
  --id A --hosts 127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183 \
  --root /hw4/manual --source work/it_vacancies_2024.csv \
  --work work/manual --log logs/manual-A.log
```

Во втором терминале заменить `--id A` на `--id B`, а журнал — на
`logs/manual-B.log`. Для нового опыта нужны новые `--root` и `--work`.
`FENCING=0` отключает защиту публикации, `FENCING=1` включает её.
