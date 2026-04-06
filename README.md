# Clickstream Analyzer

Web-сервис для анализа кликстрим-логов с интерактивными отчётами.

## Возможности

- **Аномалии** — обнаружение выбросов по Z-score
- **Тренды** — линейная регрессия по временным рядам
- **Отклонения** — Isolation Forest для выявления необычных паттернов
- **Аномальные пользователи** — анализ поведения на уровне user_id

## Формат входных данных

CSV, JSON или JSONL со следующими полями:

| Поле | Тип | Описание |
|------|-----|----------|
| timestamp | datetime | Время события |
| user_id | string | Идентификатор пользователя |
| session_id | string | Идентификатор сессии |
| event_type | string | Тип события (click, view, etc.) |
| page | string | URL или название страницы |
| duration | float | Длительность (сек) |

## Запуск

```bash
pip install -r requirements.txt
PORT=80 python3 app.py
```

Сервис запустится на `http://localhost:80`.

## Генерация тестовых данных

```bash
python3 generate_test_data.py
```

Создаёт `test_data.csv` с синтетическими логами для проверки.

## Технологии

- **Flask** — веб-фреймворк
- **pandas / numpy / scipy** — анализ данных
- **scikit-learn** — Isolation Forest
- **Plotly** — интерактивные графики в HTML-отчётах
