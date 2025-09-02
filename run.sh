#!/bin/bash

echo "🚀 Запуск скрипта объединения прайс-листов..."

# Проверяем наличие Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 не найден. Установите Python 3.7+"
    exit 1
fi

# Проверяем наличие pip
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 не найден. Установите pip"
    exit 1
fi

echo "📦 Устанавливаем зависимости..."
pip3 install -r requirements.txt

echo "🔧 Запускаем основной скрипт..."
python3 merge_pricelists.py

echo "✅ Готово!"
