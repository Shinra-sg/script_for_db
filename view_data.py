#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для просмотра данных из объединенной базы прайс-листов
Работает с новой структурой: отдельная таблица для каждого файла
"""

import sqlite3
import pandas as pd

def view_database_info():
    """Показать основную информацию о базе данных"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print("📊 ИНФОРМАЦИЯ О БАЗЕ ДАННЫХ")
    print("=" * 50)
    
    # Получаем список всех таблиц
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    # Фильтруем системные таблицы
    data_tables = [table[0] for table in tables if table[0] not in ['sqlite_sequence', 'pricelists_metadata']]
    
    print(f"Всего таблиц с данными: {len(data_tables)}")
    
    # Метаданные
    print("\n📁 МЕТАДАННЫЕ ПРАЙС-ЛИСТОВ:")
    print("-" * 50)
    cursor.execute("""
        SELECT table_name, source_file, source_sheet, row_count, column_count, file_size_mb
        FROM pricelists_metadata 
        ORDER BY row_count DESC
    """)
    
    total_rows = 0
    for row in cursor.fetchall():
        table_name, source_file, sheet, rows, cols, size = row
        total_rows += rows
        print(f"{table_name}: {source_file} ({sheet}) - {rows:,} строк, {cols} столбцов, {size:.1f} MB")
    
    print(f"\n📊 Общее количество строк: {total_rows:,}")
    
    # Структура базы данных
    print("\n🏗️  СТРУКТУРА БАЗЫ ДАННЫХ:")
    print("-" * 50)
    for table_name in data_tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"  Таблица {table_name}: {row_count:,} строк")
    
    conn.close()

def view_table_structure(table_name):
    """Показать структуру конкретной таблицы"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print(f"\n🔍 СТРУКТУРА ТАБЛИЦЫ: {table_name}")
    print("=" * 60)
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print("Столбцы:")
        for col in columns:
            col_name = col[1]
            col_type = col[2]
            print(f"  {col_name} ({col_type})")
        
        # Количество строк
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"\nКоличество строк: {row_count:,}")
        
    except Exception as e:
        print(f"Ошибка при чтении структуры таблицы: {e}")
    finally:
        conn.close()

def view_sample_data(table_name, limit=5):
    """Показать примеры данных из конкретной таблицы"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print(f"\n📋 ПРИМЕРЫ ДАННЫХ ИЗ ТАБЛИЦЫ: {table_name}")
    print("=" * 80)
    
    try:
        # Читаем данные
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            print("Таблица пуста")
        else:
            print(f"Найдено строк: {len(df)}")
            print(df.to_string(index=False, max_cols=10))
            
    except Exception as e:
        print(f"Ошибка при чтении данных: {e}")
    finally:
        conn.close()

def search_by_source(source_file, limit=5):
    """Поиск данных по конкретному файлу"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print(f"\n🔍 ПОИСК ПО ФАЙЛУ: {source_file}")
    print("=" * 80)
    
    try:
        # Ищем таблицу по метаданным
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, source_sheet, row_count 
            FROM pricelists_metadata 
            WHERE source_file LIKE ?
        """, [f'%{source_file.split()[0]}%'])
        
        results = cursor.fetchall()
        
        if not results:
            print("Файл не найден")
            return
        
        print(f"Найдено таблиц: {len(results)}")
        
        for table_name, sheet, rows in results:
            print(f"\n📋 Таблица: {table_name}")
            print(f"   Лист: {sheet}")
            print(f"   Строк: {rows:,}")
            
            # Показываем примеры данных
            try:
                query = f"SELECT * FROM {table_name} LIMIT {limit}"
                df = pd.read_sql_query(query, conn)
                
                if not df.empty:
                    # Показываем только основные столбцы для читаемости
                    display_cols = ['id', 'source_row', 'processed_at']
                    data_cols = [col for col in df.columns if col not in display_cols and not col.startswith('Unnamed')]
                    display_cols.extend(data_cols[:3])  # Максимум 3 столбца с данными
                    
                    print(f"   Примеры данных:")
                    print(df[display_cols].to_string(index=False, max_cols=10))
                    
            except Exception as e:
                print(f"   Ошибка чтения данных: {e}")
            
    except Exception as e:
        print(f"Ошибка при поиске: {e}")
    finally:
        conn.close()

def list_all_tables():
    """Показать список всех таблиц с краткой информацией"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print("\n📋 СПИСОК ВСЕХ ТАБЛИЦ:")
    print("=" * 60)
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, source_file, source_sheet, row_count, column_count, file_size_mb
            FROM pricelists_metadata 
            ORDER BY row_count DESC
        """)
        
        for row in cursor.fetchall():
            table_name, source_file, sheet, rows, cols, size = row
            print(f"🔹 {table_name}")
            print(f"   📄 Файл: {source_file}")
            print(f"   📊 Лист: {sheet}")
            print(f"   📈 Строк: {rows:,}")
            print(f"   📋 Столбцов: {cols}")
            print(f"   💾 Размер: {size:.1f} MB")
            print()
            
    except Exception as e:
        print(f"Ошибка при получении списка таблиц: {e}")
    finally:
        conn.close()

def main():
    """Главная функция"""
    print("🔍 ПРОСМОТР БАЗЫ ДАННЫХ ПРАЙС-ЛИСТОВ")
    print("=" * 60)
    
    # Показываем основную информацию
    view_database_info()
    
    # Список всех таблиц
    list_all_tables()
    
    # Пример просмотра структуры конкретной таблицы
    print("=" * 60)
    print("🔍 ПРИМЕР ПРОСМОТРА КОНКРЕТНОЙ ТАБЛИЦЫ:")
    
    # Находим первую таблицу для примера
    conn = sqlite3.connect('unified_pricelists.db')
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM pricelists_metadata LIMIT 1")
    first_table = cursor.fetchone()
    conn.close()
    
    if first_table:
        table_name = first_table[0]
        view_table_structure(table_name)
        view_sample_data(table_name, 3)
    
    # Пример поиска по конкретному файлу
    print("\n" + "=" * 60)
    search_by_source("ФЕРОН прайс 07.08.25.csv", 3)

if __name__ == "__main__":
    main()
