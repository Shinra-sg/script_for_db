#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для экспорта данных из объединенной базы прайс-листов в Excel файл
Работает с новой структурой: отдельная таблица для каждого файла
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

def export_all_tables_to_excel(output_file=None):
    """Экспорт всех таблиц в Excel файл"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"all_pricelists_export_{timestamp}.xlsx"
    
    print(f"📤 Экспорт всех таблиц в файл: {output_file}")
    
    try:
        # Подключаемся к базе
        conn = sqlite3.connect('unified_pricelists.db')
        
        # Получаем список всех таблиц с данными
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, source_file, source_sheet, row_count, column_count
            FROM pricelists_metadata 
            ORDER BY row_count DESC
        """)
        
        tables_info = cursor.fetchall()
        
        if not tables_info:
            print("❌ Таблицы не найдены")
            return False
        
        print(f"📊 Найдено таблиц: {len(tables_info)}")
        
        # Экспортируем в Excel
        print("💾 Запись в Excel файл...")
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Сводная таблица со статистикой
            stats_data = []
            for table_name, source_file, sheet, rows, cols in tables_info:
                stats_data.append({
                    'Таблица': table_name,
                    'Файл': source_file,
                    'Лист': sheet,
                    'Строк': rows,
                    'Столбцов': cols
                })
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Статистика', index=False)
            print(f"  📊 Лист 'Статистика' создан")
            
            # Экспорт каждой таблицы в отдельный лист
            for table_name, source_file, sheet, rows, cols in tables_info:
                try:
                    # Читаем данные таблицы
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql_query(query, conn)
                    
                    # Создаем безопасное имя листа (Excel ограничения)
                    safe_sheet_name = f"{table_name[:30]}"  # Ограничиваем длину
                    
                    # Записываем в Excel
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    print(f"  📋 Лист '{safe_sheet_name}' создан ({rows:,} строк)")
                    
                except Exception as e:
                    print(f"  ❌ Ошибка экспорта таблицы {table_name}: {e}")
                    continue
        
        print(f"✅ Экспорт завершен! Файл: {output_file}")
        
        # Показываем размер файла
        import os
        file_size = os.path.getsize(output_file)
        print(f"📁 Размер файла: {file_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

def export_specific_table(table_name, output_file=None):
    """Экспорт конкретной таблицы"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = table_name.replace(' ', '_').replace('-', '_')
        output_file = f"{safe_name}_export_{timestamp}.xlsx"
    
    print(f"📤 Экспорт таблицы {table_name} в: {output_file}")
    
    try:
        conn = sqlite3.connect('unified_pricelists.db')
        
        # Проверяем, существует ли таблица
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table_name])
        
        if not cursor.fetchone():
            print(f"❌ Таблица {table_name} не найдена")
            return False
        
        # Получаем информацию о таблице
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"📊 Найдено строк: {row_count:,}")
        
        # Читаем данные
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        
        # Экспортируем
        df.to_excel(output_file, index=False, sheet_name='Данные')
        print(f"✅ Экспорт завершен! Файл: {output_file}")
        
        # Показываем размер файла
        import os
        file_size = os.path.getsize(output_file)
        print(f"📁 Размер файла: {file_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

def export_by_source(source_file, output_file=None):
    """Экспорт данных конкретного файла по метаданным"""
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = source_file.replace(' ', '_').replace('.', '_')
        output_file = f"{safe_name}_export_{timestamp}.xlsx"
    
    print(f"📤 Экспорт файла {source_file} в: {output_file}")
    
    try:
        conn = sqlite3.connect('unified_pricelists.db')
        
        # Ищем таблицы по метаданным
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, source_sheet, row_count 
            FROM pricelists_metadata 
            WHERE source_file LIKE ?
        """, [f'%{source_file.split()[0]}%'])
        
        results = cursor.fetchall()
        
        if not results:
            print("❌ Файл не найден")
            return False
        
        print(f"📊 Найдено таблиц: {len(results)}")
        
        # Экспортируем в Excel с несколькими листами
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # Сводная информация
            summary_data = []
            for table_name, sheet, rows in results:
                summary_data.append({
                    'Таблица': table_name,
                    'Лист': sheet,
                    'Строк': rows
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Сводка', index=False)
            print(f"  📊 Лист 'Сводка' создан")
            
            # Экспорт каждой таблицы
            for table_name, sheet, rows in results:
                try:
                    # Читаем данные
                    query = f"SELECT * FROM {table_name}"
                    df = pd.read_sql_query(query, conn)
                    
                    # Создаем безопасное имя листа
                    safe_sheet_name = f"{sheet[:30]}" if sheet != "CSV" else f"{table_name[:30]}"
                    
                    # Записываем в Excel
                    df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    print(f"  📋 Лист '{safe_sheet_name}' создан ({rows:,} строк)")
                    
                except Exception as e:
                    print(f"  ❌ Ошибка экспорта таблицы {table_name}: {e}")
                    continue
        
        print(f"✅ Экспорт завершен! Файл: {output_file}")
        
        # Показываем размер файла
        import os
        file_size = os.path.getsize(output_file)
        print(f"📁 Размер файла: {file_size / (1024*1024):.1f} MB")
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

def list_available_tables():
    """Показать список доступных таблиц"""
    conn = sqlite3.connect('unified_pricelists.db')
    
    print("📋 ДОСТУПНЫЕ ТАБЛИЦЫ ДЛЯ ЭКСПОРТА:")
    print("=" * 60)
    
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name, source_file, source_sheet, row_count, column_count
            FROM pricelists_metadata 
            ORDER BY row_count DESC
        """)
        
        for row in cursor.fetchall():
            table_name, source_file, sheet, rows, cols = row
            print(f"🔹 {table_name}")
            print(f"   📄 Файл: {source_file}")
            print(f"   📊 Лист: {sheet}")
            print(f"   📈 Строк: {rows:,}")
            print(f"   📋 Столбцов: {cols}")
            print()
            
    except Exception as e:
        print(f"Ошибка при получении списка таблиц: {e}")
    finally:
        conn.close()

def main():
    """Главная функция"""
    print("📤 ЭКСПОРТ ДАННЫХ В EXCEL")
    print("=" * 50)
    
    # Показываем доступные таблицы
    list_available_tables()
    
    # Экспорт всех таблиц
    print("1️⃣ Экспорт всех таблиц...")
    export_all_tables_to_excel()
    
    print("\n" + "=" * 50)
    
    # Экспорт по конкретному файлу (пример)
    print("2️⃣ Экспорт конкретного файла (пример)...")
    export_by_source("ФЕРОН прайс 07.08.25.csv")
    
    print("\n" + "=" * 50)
    
    # Экспорт конкретной таблицы (пример)
    print("3️⃣ Экспорт конкретной таблицы (пример)...")
    # Находим первую таблицу для примера
    conn = sqlite3.connect('unified_pricelists.db')
    cursor = conn.cursor()
    cursor.execute("SELECT table_name FROM pricelists_metadata LIMIT 1")
    first_table = cursor.fetchone()
    conn.close()
    
    if first_table:
        table_name = first_table[0]
        export_specific_table(table_name)
    
    print("\n🎉 Экспорт завершен!")

def export_unified_table():
    """Экспорт итоговой таблицы с разделителями"""
    try:
        conn = sqlite3.connect('unified_pricelists.db')
        cursor = conn.cursor()
        
        # Получаем данные из итоговой таблицы
        cursor.execute("SELECT * FROM all_pricelists ORDER BY id")
        rows = cursor.fetchall()
        
        if not rows:
            print("❌ Итоговая таблица пуста")
            return
        
        # Получаем названия столбцов
        cursor.execute("PRAGMA table_info(all_pricelists)")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]
        
        # Создаем DataFrame
        df = pd.DataFrame(rows, columns=column_names)
        
        # Создаем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"unified_pricelists_export_{timestamp}.xlsx"
        
        print(f"📤 Экспорт итоговой таблицы в файл: {filename}")
        print(f"📊 Найдено строк: {len(df):,}")
        
        # Создаем Excel файл
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Основной лист с данными
            df.to_excel(writer, sheet_name='Все_данные', index=False)
            
            # Лист со статистикой по разделам
            separators_df = df[df['section_separator'].notna()][['section_separator']].drop_duplicates()
            separators_df.to_excel(writer, sheet_name='Разделы', index=False)
            
            # Лист со статистикой по источникам
            source_stats = df[df['section_separator'].isna()].groupby(['source_file', 'sheet_name']).size().reset_index(name='Количество_записей')
            source_stats.to_excel(writer, sheet_name='Статистика_по_источникам', index=False)
        
        # Получаем размер файла
        file_size = os.path.getsize(filename) / (1024 * 1024)  # MB
        print(f"✅ Экспорт завершен! Файл: {filename}")
        print(f"📁 Размер файла: {file_size:.1f} MB")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка экспорта итоговой таблицы: {e}")

if __name__ == "__main__":
    main()
    
    # Дополнительный экспорт итоговой таблицы
    print("\n" + "=" * 50)
    print("4️⃣ Экспорт итоговой таблицы с разделителями...")
    export_unified_table()
