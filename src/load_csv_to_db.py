import argparse
import csv
import os
import re
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_batch
from dotenv import load_dotenv


# Шаблон допустимых файлов: {{shop_num}}_{{cash_num}}.csv
FILENAME_PATTERN = re.compile(r"^(\d+)_(\d+)\.csv$")


def load_env() -> None:
    """
    Загружает переменные окружения из .env в корне проекта.
    Скрипт лежит в src/, поэтому поднимаемся на один уровень выше.
    """
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"
    load_dotenv(env_path)


def get_db_connection() -> PgConnection:
    """
    Создаёт подключение к PostgreSQL, используя переменные окружения.
    """
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([dbname, user, password]):
        raise RuntimeError(
            "Не заданы DB_NAME / DB_USER / DB_PASSWORD в .env. "
            "Проверь файл .env в корне проекта."
        )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    return conn


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Загрузка CSV-файлов продаж в PostgreSQL."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Папка с CSV-файлами (по умолчанию ./data).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не загружать в базу, только разобрать файлы и вывести статистику.",
    )
    return parser.parse_args()


def find_csv_files(data_dir: Path) -> List[Tuple[Path, int, int]]:
    """
    Находит все файлы вида {{shop_num}}_{{cash_num}}.csv в указанной папке.
    Возвращает список троек: (путь, shop_num, cash_num).
    """
    result: List[Tuple[Path, int, int]] = []

    for entry in data_dir.iterdir():
        if not entry.is_file():
            continue

        match = FILENAME_PATTERN.match(entry.name)
        if not match:
            # Просто игнорируем "лишние" файлы.
            print(f"Пропускаю файл с неподходящим именем: {entry.name}")
            continue

        shop_num = int(match.group(1))
        cash_num = int(match.group(2))
        result.append((entry, shop_num, cash_num))

    return result


def read_csv_file(
    filepath: Path,
    shop_num: int,
    cash_num: int,
) -> List[Tuple]:
    """
    Читает CSV и преобразует строки к типам, подходящим для вставки в БД.
    Возвращает список кортежей со значениями полей.
    """
    rows: List[Tuple] = []

    with filepath.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required_fields = {"doc_id", "item", "category", "amount", "price", "discount"}
        if not required_fields.issubset(reader.fieldnames or []):
            raise ValueError(
                f"Файл {filepath.name} не содержит всех нужных колонок: {required_fields}"
            )

        for line_num, row in enumerate(reader, start=2):
            try:
                doc_id = row["doc_id"].strip()
                item = row["item"].strip()
                category = row["category"].strip()
                amount = int(row["amount"])
                price = Decimal(row["price"])
                discount = Decimal(row["discount"])
            except Exception as exc:
                raise ValueError(
                    f"Ошибка парсинга строки {line_num} в файле {filepath.name}: {exc}"
                ) from exc

            rows.append(
                (
                    doc_id,
                    item,
                    category,
                    amount,
                    price,
                    discount,
                    shop_num,
                    cash_num,
                    filepath.name,
                )
            )

    return rows


def insert_rows(
    conn: PgConnection,
    rows: List[Tuple],
) -> None:
    """
    Вставляет строки в таблицу sales.
    """
    sql = """
        INSERT INTO public.sales (
            doc_id,
            item,
            category,
            amount,
            price,
            discount,
            shop_num,
            cash_num,
            file_name
        )
        VALUES (
            %(doc_id)s,
            %(item)s,
            %(category)s,
            %(amount)s,
            %(price)s,
            %(discount)s,
            %(shop_num)s,
            %(cash_num)s,
            %(file_name)s
        );
    """
    # Для простоты будем использовать execute_batch c позиционными параметрами.
    sql = """
        INSERT INTO public.sales (
            doc_id,
            item,
            category,
            amount,
            price,
            discount,
            shop_num,
            cash_num,
            file_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=1000)


def move_file_to_processed(filepath: Path, processed_dir: Path) -> None:
    """
    Перемещает успешно обработанный файл в папку processed.
    """
    processed_dir.mkdir(parents=True, exist_ok=True)
    target = processed_dir / filepath.name
    filepath.rename(target)
    print(f"Файл {filepath.name} перемещён в {target}")


def process_files(
    data_dir: Path,
    conn: Optional[PgConnection],
    dry_run: bool = False,
) -> None:
    """
    Основная логика:
    - ищем файлы
    - читаем
    - при необходимости грузим в БД
    - переносим в processed
    """
    processed_dir = data_dir / "processed"
    files = find_csv_files(data_dir)

    if not files:
        print(f"В папке {data_dir} нет файлов для обработки.")
        return

    print(f"Найдено файлов для обработки: {len(files)}")

    for filepath, shop_num, cash_num in files:
        print(f"Обрабатываю файл {filepath.name} (магазин {shop_num}, касса {cash_num})...")
        rows = read_csv_file(filepath, shop_num, cash_num)
        print(f"  строк для загрузки: {len(rows)}")

        if dry_run:
            print("  Режим dry-run: в БД не загружаем, файл не перемещаем.")
            continue

        assert conn is not None, "Подключение к БД не должно быть None, если dry_run=False"

        try:
            with conn:
                insert_rows(conn, rows)
            move_file_to_processed(filepath, processed_dir)
        except Exception as exc:
            # В контексте "with conn" при Exception транзакция будет откатана.
            print(f"  ОШИБКА при загрузке файла {filepath.name}: {exc}")
            # Файл оставляем на месте, чтобы можно было разобраться.


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)

    if not data_dir.exists():
        raise FileNotFoundError(f"Папка с данными не найдена: {data_dir}")

    load_env()

    if args.dry_run:
        print("Запуск в режиме dry-run (без подключения к БД)...")
        process_files(data_dir, conn=None, dry_run=True)
    else:
        conn = get_db_connection()
        print("Подключение к БД установлено.")
        try:
            process_files(data_dir, conn=conn, dry_run=False)
        finally:
            conn.close()
            print("Подключение к БД закрыто.")


if __name__ == "__main__":
    main()
