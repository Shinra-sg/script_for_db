#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для объединения прайс-листов в единую базу данных
Поддерживает форматы: .xlsx, .xls, .xlsm, .csv
Каждый файл создает отдельную таблицу в базе данных
"""

import os
import sqlite3
import pandas as pd
import glob
from pathlib import Path
import logging
from datetime import datetime
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merge_pricelists.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PricelistMerger:
    def __init__(self, doc_folder="doc", output_db="unified_pricelists.db"):
        self.doc_folder = doc_folder
        self.output_db = output_db
        self.conn = None
        self.cursor = None
        
    def connect_db(self):
        """Создание подключения к базе данных"""
        try:
            self.conn = sqlite3.connect(self.output_db)
            self.cursor = self.conn.cursor()
            logger.info(f"Подключение к базе данных {self.output_db} установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise
    
    def create_metadata_table(self):
        """Создание таблицы с метаданными о всех прайс-листах"""
        try:
            create_sql = """
            CREATE TABLE IF NOT EXISTS pricelists_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT UNIQUE NOT NULL,
                source_file TEXT NOT NULL,
                source_sheet TEXT,
                row_count INTEGER,
                column_count INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_size_mb REAL,
                file_format TEXT
            )
            """
            self.cursor.execute(create_sql)
            self.conn.commit()
            logger.info("Таблица метаданных создана")
        except Exception as e:
            logger.error(f"Ошибка создания таблицы метаданных: {e}")
            raise
    
    def create_unified_table(self):
        """Создание итоговой таблицы со всеми данными"""
        try:
            # Сначала создаем базовую структуру
            create_sql = """
            CREATE TABLE IF NOT EXISTS all_pricelists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT NOT NULL,
                sheet_name TEXT,
                source_row INTEGER,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            self.cursor.execute(create_sql)
            self.conn.commit()
            logger.info("Итоговая таблица all_pricelists создана")
        except Exception as e:
            logger.error(f"Ошибка создания итоговой таблицы: {e}")
            raise
    
    def add_column_to_unified_table(self, column_name):
        """Добавление столбца в итоговую таблицу"""
        try:
            clean_column = self.clean_column_name(column_name)
            self.cursor.execute(f"ALTER TABLE all_pricelists ADD COLUMN {clean_column} TEXT")
            self.conn.commit()
        except Exception as e:
            # Столбец уже существует, игнорируем ошибку
            pass
    
    def insert_to_unified_table(self, df, file_path, sheet_name):
        """Вставка данных в итоговую таблицу"""
        try:
            # Добавляем недостающие столбцы
            for col in df.columns:
                self.add_column_to_unified_table(col)
            
            # Вставляем данные
            for index, row in df.iterrows():
                # Подготавливаем данные для вставки
                insert_data = {
                    'source_file': os.path.basename(file_path),
                    'sheet_name': sheet_name,
                    'source_row': index + 1
                }
                
                # Добавляем данные из строки
                for col in df.columns:
                    clean_col = self.clean_column_name(col)
                    insert_data[clean_col] = str(row[col]) if pd.notna(row[col]) else None
                
                # Формируем SQL запрос
                columns = list(insert_data.keys())
                placeholders = ['?' for _ in insert_data]
                
                insert_sql = f"""
                INSERT INTO all_pricelists ({', '.join(columns)})
                VALUES ({', '.join(placeholders)})
                """
                
                self.cursor.execute(insert_sql, list(insert_data.values()))
            
            self.conn.commit()
            logger.info(f"  Данные добавлены в итоговую таблицу all_pricelists")
            
        except Exception as e:
            logger.error(f"Ошибка добавления данных в итоговую таблицу: {e}")
            raise
    
    def get_safe_table_name(self, file_path, sheet_name=None):
        """Создание безопасного имени таблицы для SQLite"""
        # Получаем имя файла без расширения
        file_name = Path(file_path).stem
        
        # Добавляем название листа, если есть
        if sheet_name and sheet_name != "CSV":
            table_name = f"{file_name}_{sheet_name}"
        else:
            table_name = file_name
        
        # Очищаем от недопустимых символов
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
        
        # Убираем множественные подчеркивания
        table_name = re.sub(r'_+', '_', table_name)
        
        # Убираем подчеркивания в начале и конце
        table_name = table_name.strip('_')
        
        # Убираем цифры в начале (SQLite не позволяет)
        table_name = re.sub(r'^[0-9]+', '', table_name)
        table_name = table_name.strip('_')
        
        # Ограничиваем длину
        if len(table_name) > 50:
            table_name = table_name[:50]
        
        # Если название пустое, даем стандартное
        if not table_name:
            table_name = "unnamed_table"
        
        # Добавляем префикс, если название начинается с цифры
        if table_name and table_name[0].isdigit():
            table_name = f"table_{table_name}"
        
        return table_name
    
    def create_table_for_file(self, table_name, df, file_path, sheet_name):
        """Создание таблицы для конкретного файла/листа"""
        try:
            # Создаем таблицу с динамическими столбцами
            columns = []
            existing_columns = set()
            for col in df.columns:
                clean_col = self.clean_column_name(col, existing_columns)
                columns.append(f"{clean_col} TEXT")
            
            # Добавляем системные столбцы
            columns.insert(0, "id INTEGER PRIMARY KEY AUTOINCREMENT")
            columns.insert(1, "source_row INTEGER")
            columns.insert(2, "processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
            """
            
            self.cursor.execute(create_sql)
            self.conn.commit()
            logger.info(f"  Создана таблица: {table_name}")
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы {table_name}: {e}")
            raise
    
    def insert_data_to_table(self, table_name, df, file_path, sheet_name):
        """Вставка данных в таблицу"""
        try:
            # Получаем список столбцов таблицы
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            table_columns = [col[1] for col in self.cursor.fetchall()]
            
            # Подготавливаем данные для вставки
            for index, row in df.iterrows():
                insert_data = {
                    'source_row': index + 1
                }
                
                # Добавляем данные из строки
                for col in df.columns:
                    clean_col = self.clean_column_name(col)
                    if clean_col in table_columns:
                        value = row[col]
                        
                        # Обрабатываем NaN значения
                        if pd.isna(value):
                            value = None
                        elif isinstance(value, (int, float)):
                            # Оставляем числа как есть
                            pass
                        else:
                            # Преобразуем в строку
                            value = str(value)
                        
                        insert_data[clean_col] = value
                
                # Формируем SQL запрос
                columns = list(insert_data.keys())
                placeholders = ', '.join(['?' for _ in columns])
                insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Выполняем вставку
                self.cursor.execute(insert_sql, list(insert_data.values()))
            
            self.conn.commit()
            logger.info(f"  Добавлено {len(df)} строк в таблицу {table_name}")
            
        except Exception as e:
            logger.error(f"Ошибка вставки данных в таблицу {table_name}: {e}")
            self.conn.rollback()
            raise
    
    def process_excel_file(self, file_path):
        """Обработка Excel файла (.xlsx, .xls, .xlsm)"""
        try:
            logger.info(f"Обработка файла: {file_path}")
            
            # Получаем размер файла
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            file_format = Path(file_path).suffix.lower()
            
            # Читаем все листы из файла
            excel_file = pd.ExcelFile(file_path)
            
            for sheet_name in excel_file.sheet_names:
                logger.info(f"  Обработка листа: {sheet_name}")
                
                # Читаем лист без заголовков для определения структуры
                raw_df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                
                if raw_df.empty:
                    logger.warning(f"  Лист {sheet_name} пустой, пропускаем")
                    continue
                
                # Определяем, где начинаются реальные данные
                header_row_index = self.detect_data_start_row(raw_df)
                
                # Теперь читаем лист с правильными заголовками
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row_index)
                
                if df.empty:
                    logger.warning(f"  После обработки заголовков лист {sheet_name} пустой, пропускаем")
                    continue
                
                # Создаем имя таблицы
                table_name = self.get_safe_table_name(file_path, sheet_name)
                
                # Создаем таблицу
                self.create_table_for_file(table_name, df, file_path, sheet_name)
                
                # Вставляем данные
                self.insert_data_to_table(table_name, df, file_path, sheet_name)
                
                # Добавляем данные в итоговую таблицу
                self.insert_to_unified_table(df, file_path, sheet_name)
                
                # Добавляем метаданные
                self.add_metadata(table_name, file_path, sheet_name, len(df), len(df.columns), file_size_mb, file_format)
                
        except Exception as e:
            logger.error(f"Ошибка обработки Excel файла {file_path}: {e}")
    
    def process_csv_file(self, file_path):
        """Обработка CSV файла"""
        try:
            logger.info(f"Обработка CSV файла: {file_path}")
            
            # Получаем размер файла
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
            file_format = "csv"
            
            # Пробуем разные кодировки
            encodings = ['utf-8', 'cp1251', 'windows-1251', 'iso-8859-1']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    logger.info(f"  Успешно прочитан с кодировкой: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"Не удалось прочитать CSV файл {file_path} ни с одной кодировкой")
                return
            
            if df.empty:
                logger.warning(f"CSV файл {file_path} пустой")
                return
            
            # Создаем имя таблицы
            table_name = self.get_safe_table_name(file_path, "CSV")
            
            # Создаем таблицу
            self.create_table_for_file(table_name, df, file_path, "CSV")
            
            # Вставляем данные
            self.insert_data_to_table(table_name, df, file_path, "CSV")
            
            # Добавляем данные в итоговую таблицу
            self.insert_to_unified_table(df, file_path, "CSV")
            
            # Добавляем метаданные
            self.add_metadata(table_name, file_path, "CSV", len(df), len(df.columns), file_size_mb, file_format)
            
        except Exception as e:
            logger.error(f"Ошибка обработки CSV файла {file_path}: {e}")
    
    def add_metadata(self, table_name, file_path, sheet_name, row_count, column_count, file_size_mb, file_format):
        """Добавление метаданных о таблице"""
        try:
            file_name = os.path.basename(file_path)
            
            insert_sql = """
            INSERT OR REPLACE INTO pricelists_metadata 
            (table_name, source_file, source_sheet, row_count, column_count, file_size_mb, file_format)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            
            self.cursor.execute(insert_sql, [
                table_name, file_name, sheet_name, row_count, column_count, file_size_mb, file_format
            ])
            
            self.conn.commit()
            logger.info(f"  Метаданные добавлены для таблицы {table_name}")
            
        except Exception as e:
            logger.error(f"Ошибка добавления метаданных: {e}")
    
    def clean_column_name(self, column_name, existing_columns=None):
        """Очистка названия столбца от недопустимых символов для SQLite"""
        if column_name is None:
            return "unnamed_column"
        
        # Преобразуем в строку
        column_name = str(column_name)
        
        # Убираем недопустимые символы
        invalid_chars = [' ', '-', '.', '(', ')', '[', ']', '{', '}', '!', '@', '#', '$', '%', '^', '&', '*', '+', '=', '|', '\\', '/', ':', ';', '"', "'", ',', '<', '>', '?']
        
        for char in invalid_chars:
            column_name = column_name.replace(char, '_')
        
        # Убираем множественные подчеркивания
        while '__' in column_name:
            column_name = column_name.replace('__', '_')
        
        # Убираем подчеркивания в начале и конце
        column_name = column_name.strip('_')
        
        # Если название пустое, даем стандартное
        if not column_name:
            column_name = "unnamed_column"
        
        # Убираем цифры в начале (SQLite не позволяет)
        column_name = re.sub(r'^[0-9]+', '', column_name)
        column_name = column_name.strip('_')
        
        # Если название пустое после удаления цифр, даем стандартное
        if not column_name:
            column_name = "unnamed_column"
        
        # Ограничиваем длину названия
        if len(column_name) > 50:
            column_name = column_name[:50]
        
        # Проверяем на дублирование и добавляем суффикс
        if existing_columns is not None:
            original_name = column_name
            counter = 1
            while column_name in existing_columns:
                column_name = f"{original_name}_{counter}"
                counter += 1
            existing_columns.add(column_name)
        
        return column_name
    
    def detect_data_start_row(self, df):
        """Определение строки, с которой начинаются реальные данные"""
        try:
            # Ищем строку с заголовками таблицы
            for i in range(min(30, len(df))):  # Проверяем первые 30 строк
                row = df.iloc[i]
                
                # Расширенный список типичных заголовков таблицы
                typical_headers = [
                    # Основные заголовки
                    'артикул', 'наименование', 'название', 'код', 'номер', 'цена', 'стоимость', 
                    'количество', 'ед', 'шт', 'руб', 'usd', 'eur', 'category', 'категория',
                    'partnumber', 'описание', 'характеристики', 'вес', 'объем', 'номенклатура',
                    
                    # Дополнительные заголовки
                    'заказ', 'товар', 'продукт', 'изделие', 'модель', 'марка', 'бренд',
                    'производитель', 'поставщик', 'материал', 'цвет', 'размер', 'длина',
                    'ширина', 'высота', 'диаметр', 'мощность', 'напряжение', 'ток',
                    'частота', 'температура', 'давление', 'скорость', 'время', 'дата',
                    
                    # Финансовые заголовки
                    'сумма', 'итого', 'скидка', 'наценка', 'налог', 'пошлина',
                    'доставка', 'упаковка', 'гарантия', 'сервис', 'обслуживание',
                    
                    # Единицы измерения
                    'кг', 'г', 'л', 'мл', 'м', 'см', 'мм', 'кв.м', 'куб.м',
                    'шт', 'компл', 'упак', 'пачка', 'коробка', 'ящик', 'мешок',
                    
                    # Статусы и состояния
                    'статус', 'состояние', 'наличие', 'остаток', 'склад', 'место',
                    'адрес', 'контакт', 'телефон', 'email', 'сайт', 'информация'
                ]
                
                # Считаем количество найденных типичных заголовков
                found_headers = 0
                header_matches = []
                
                for val in row:
                    if pd.notna(val) and isinstance(val, str):
                        val_lower = val.lower().strip()
                        for header in typical_headers:
                            if header in val_lower:
                                found_headers += 1
                                header_matches.append(val)
                                break
                
                # Дополнительная проверка: ищем строки с явными признаками заголовков
                # Если в строке есть слова "Заказ", "Название" и подобные - это точно заголовки
                strong_headers = ['заказ', 'название', 'артикул', 'код', 'цена', 'номенклатура']
                strong_matches = 0
                
                for val in row:
                    if pd.notna(val) and isinstance(val, str):
                        val_lower = val.lower().strip()
                        for strong_header in strong_headers:
                            if strong_header in val_lower:
                                strong_matches += 1
                                break
                
                # Проверка на служебные слова, которые НЕ должны быть заголовками
                service_words = [
                    'электрорешения', 'устойчивого', 'будущего', 'калькуляторы', 'конфигураторы',
                    'артикулов', 'собственных', 'производственных', 'комплекса', 'итого',
                    'зеленый', 'цвет', 'наименования', 'позиция', 'ожидается', 'указанную',
                    'дату', 'оранжевый', 'новинки', 'экспресс', 'доставка', 'отфильтруйте',
                    'непустые', 'значения', 'полю', 'ims', 'нажмите', 'загрузить', 'xls'
                ]
                
                service_count = 0
                for val in row:
                    if pd.notna(val) and isinstance(val, str):
                        val_lower = val.lower().strip()
                        for service_word in service_words:
                            if service_word in val_lower:
                                service_count += 1
                                break
                
                # Если найдено много служебных слов - это НЕ заголовки
                if service_count > 2:
                    continue
                
                # Если найдено минимум 3 типичных заголовка ИЛИ минимум 2 сильных заголовка - это заголовки таблицы
                if found_headers >= 3 or strong_matches >= 2:
                    logger.info(f"    Заголовок таблицы найден в строке {i+1} (найдено {found_headers} типичных, {strong_matches} сильных заголовков)")
                    if header_matches:
                        logger.info(f"    Найденные заголовки: {', '.join(header_matches[:5])}...")
                    return i  # Возвращаем САМУ строку с заголовками (не следующую!)
            
            # Если заголовок не найден, возвращаем 0 (начинаем с первой строки)
            logger.info(f"    Заголовок таблицы не найден, начинаем с первой строки")
            return 0
            
        except Exception as e:
            logger.error(f"Ошибка определения заголовка: {e}")
            return 0
    
    def clean_dataframe(self, df, start_row):
        """Очистка DataFrame от служебной информации"""
        try:
            if start_row > 0:
                # Убираем строки до заголовка
                df_cleaned = df.iloc[start_row:].copy()
                logger.info(f"    Убрано {start_row} служебных строк")
                return df_cleaned
            return df
        except Exception as e:
            logger.error(f"Ошибка очистки DataFrame: {e}")
            return df
    
    def get_all_files(self):
        """Получение списка всех файлов для обработки"""
        files = []
        
        # Excel файлы
        excel_patterns = ['*.xlsx', '*.xls', '*.xlsm']
        for pattern in excel_patterns:
            files.extend(glob.glob(os.path.join(self.doc_folder, pattern)))
        
        # CSV файлы
        csv_files = glob.glob(os.path.join(self.doc_folder, '*.csv'))
        files.extend(csv_files)
        
        return sorted(files)
    
    def merge_all_pricelists(self):
        """Основной метод для объединения всех прайс-листов"""
        try:
            logger.info("Начинаем объединение прайс-листов")
            
            # Подключаемся к базе данных
            self.connect_db()
            
            # Создаем таблицу метаданных
            self.create_metadata_table()
            
            # Создаем итоговую таблицу
            self.create_unified_table()
            
            # Получаем список файлов
            files = self.get_all_files()
            logger.info(f"Найдено файлов для обработки: {len(files)}")
            
            if not files:
                logger.warning("Файлы для обработки не найдены!")
                return
            
            # Обрабатываем каждый файл
            for file_path in files:
                logger.info(f"Обработка файла: {file_path}")
                
                file_ext = Path(file_path).suffix.lower()
                
                if file_ext in ['.xlsx', '.xls', '.xlsm']:
                    self.process_excel_file(file_path)
                elif file_ext == '.csv':
                    self.process_csv_file(file_path)
                else:
                    logger.warning(f"Неподдерживаемый формат файла: {file_ext}")
            
            # Получаем статистику
            self.cursor.execute("SELECT COUNT(*) FROM pricelists_metadata")
            total_tables = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT SUM(row_count) FROM pricelists_metadata")
            total_rows = self.cursor.fetchone()[0] or 0
            
            logger.info(f"Обработка завершена!")
            logger.info(f"Создано таблиц: {total_tables}")
            logger.info(f"Всего строк: {total_rows:,}")
            
        except Exception as e:
            logger.error(f"Ошибка в процессе объединения: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()
                logger.info("Соединение с базой данных закрыто")
    
    def show_database_info(self):
        """Показать информацию о созданной базе данных"""
        try:
            self.connect_db()
            
            # Информация о таблицах
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = self.cursor.fetchall()
            
            logger.info("Структура базы данных:")
            for table in tables:
                table_name = table[0]
                if table_name != 'sqlite_sequence':  # Пропускаем системные таблицы
                    self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = self.cursor.fetchone()[0]
                    logger.info(f"  Таблица {table_name}: {row_count} строк")
            
            # Метаданные
            logger.info("\nМетаданные прайс-листов:")
            self.cursor.execute("""
                SELECT table_name, source_file, source_sheet, row_count, column_count, file_size_mb
                FROM pricelists_metadata 
                ORDER BY row_count DESC
            """)
            metadata = self.cursor.fetchall()
            
            for row in metadata:
                table_name, source_file, sheet, rows, cols, size = row
                logger.info(f"  {table_name}: {source_file} ({sheet}) - {rows:,} строк, {cols} столбцов, {size:.1f} MB")
            
        except Exception as e:
            logger.error(f"Ошибка получения информации о базе данных: {e}")
        finally:
            if self.conn:
                self.conn.close()

def main():
    """Главная функция"""
    try:
        # Создаем экземпляр класса
        merger = PricelistMerger()
        
        # Объединяем все прайс-листы
        merger.merge_all_pricelists()
        
        # Показываем информацию о созданной базе
        merger.show_database_info()
        
        print(f"\n✅ Готово! База данных создана: {merger.output_db}")
        print("Лог файл: merge_pricelists.log")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        print(f"❌ Ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
