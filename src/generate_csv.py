import argparse
import csv
import logging
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple


ITEMS_BY_CATEGORY = {
    "бытовая химия": [
        "Стиральный порошок",
        "Гель для стирки",
        "Средство для мытья посуды",
        "Чистящее средство для ванной",
        "Универсальный очиститель",
    ],
    "текстиль": [
        "Полотенце махровое",
        "Комплект постельного белья",
        "Скатерть",
        "Плед флисовый",
    ],
    "кухонная утварь": [
        "Сковорода",
        "Кастрюля",
        "Разделочная доска",
        "Нож кухонный",
        "Набор столовых приборов",
    ],
    "товары для дома": [
        "Ведро",
        "Контейнер для хранения",
        "Корзина для белья",
        "Вешалки для одежды",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Генератор CSV-выгрузок по магазинами кассам."
    )
    parser.add_argument(
        "--n-shops",
        type=int,
        required=True,
        help="Количество магазинов.",
    )
    parser.add_argument(
        "--min-cash",
        type=int,
        default=1,
        help="Минимальное количество касс (по умолчанию 1).",
    )
    parser.add_argument(
        "--max-cash",
        type=int,
        default=3,
        help="Максимальное количество касс (по умолчанию 3).",
    )
    parser.add_argument(
        "--min-checks",
        type=int,
        default=20,
        help="Минимум чеков на кассу (по умолчанию 20).",
    )
    parser.add_argument(
        "--max-checks",
        type=int,
        default=50,
        help="Максимум чеков на кассу (по умолчанию 50).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="Папка для выгрузок (по умолчанию ./data).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Зерно для генератора случайных чисел.",
    )
    parser.add_argument(
        "--days-to-keep",
        type=int,
        default=1,
        help="Сколько дней хранить файлы (по умолчанию 1 день).",
    )

    return parser.parse_args()


def ensure_output_dir(path_str: str) -> Path:
    path = Path(path_str)
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_doc_id(length: int = 10) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(length))


def choose_item_and_category() -> Tuple[str, str]:
    category = random.choice(list(ITEMS_BY_CATEGORY.keys()))
    item = random.choice(ITEMS_BY_CATEGORY[category])
    return item, category


def generate_rows_for_cash(
    shop_num: int,
    cash_num: int,
    min_checks: int,
    max_checks: int,
) -> List[List[str]]:
    rows: List[List[str]] = []
    n_checks = random.randint(min_checks, max_checks)

    for _ in range(n_checks):
        doc_id = generate_doc_id()
        n_items = random.randint(1, 5)

        for _ in range(n_items):
            item, category = choose_item_and_category()
            amount = random.randint(1, 5)
            price = round(random.uniform(50, 3000), 2)

            if random.random() < 0.3:
                max_discount = price * amount * 0.3
                discount = round(random.uniform(0, max_discount), 2)
            else:
                discount = 0.0

            rows.append([
                doc_id,
                item,
                category,
                str(amount),
                f"{price:.2f}",
                f"{discount:.2f}",
            ])

    return rows


def write_csv_file(
        output_dir: Path,
        shop_num: int,
        cash_num: int,
        rows: List[List[str]]
        ) -> None:
    filename = output_dir / f"{shop_num}_{cash_num}.csv"
    with filename.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["doc_id",
                         "item",
                         "category",
                         "amount",
                         "price",
                         "discount"])
        writer.writerows(rows)

    print(f"Сгенерирован файл: {filename}")
    logging.info("Сгенерирован файл: %s (строк: %d)", filename, len(rows))


def cleanup_old_files(output_dir: Path, days_to_keep: int) -> None:
    now = datetime.now()
    cutoff = now - timedelta(days=days_to_keep)

    deleted = 0

    for csv_file in output_dir.glob("*.csv"):
        mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
        if mtime < cutoff:
            logging.info("Удаляю старый файл: %s (mtime=%s)", csv_file, mtime)
            csv_file.unlink()
            deleted += 1

    logging.info("Очистка завершена. Удалено файлов: %d. Храним %d дней.",
                 deleted, days_to_keep)


def main() -> None:
    args = parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    output_dir = ensure_output_dir(args.output_dir)

    # Логирование
    log_file = output_dir / "generator.log"
    logging.basicConfig(
        filename=str(log_file),
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        encoding="utf-8",
    )

    logging.info("=== Запуск генерации ===")
    logging.info("Параметры запуска: %s", args)

    # Генерация
    for shop_num in range(1, args.n_shops + 1):
        n_cashes = random.randint(args.min_cash, args.max_cash)
        logging.info("Магазин %d: касс %d", shop_num, n_cashes)

        for cash_num in range(1, n_cashes + 1):
            rows = generate_rows_for_cash(
                shop_num=shop_num,
                cash_num=cash_num,
                min_checks=args.min_checks,
                max_checks=args.max_checks,
            )
            write_csv_file(output_dir, shop_num, cash_num, rows)

    # Очистка старых файлов
    cleanup_old_files(output_dir, days_to_keep=args.days_to_keep)

    logging.info("Генерация завершена успешно.")


if __name__ == "__main__":
    main()
