#!/usr/bin/env python3
"""
Скрипт для очистки зависших процессов Chromium
Используйте для очистки процессов, оставшихся после аварийного завершения парсера
"""

import os
import subprocess
import signal
import psutil
import time

def find_chromium_processes():
    """Находит все процессы Chromium"""
    chromium_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info']):
        try:
            if proc.info['name'] and 'chromium' in proc.info['name'].lower():
                chromium_processes.append(proc)
            elif proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline']).lower()
                if 'chromium' in cmdline or 'chrome' in cmdline:
                    chromium_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    
    return chromium_processes

def kill_process_tree(pid):
    """Убивает процесс и все его дочерние процессы"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        
        # Сначала убиваем дочерние процессы
        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        
        # Даем время на корректное завершение
        gone, alive = psutil.wait_procs(children, timeout=3)
        
        # Принудительно убиваем оставшиеся
        for child in alive:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        
        # Убиваем родительский процесс
        parent.terminate()
        parent.wait(timeout=3)
        
        return True
    except psutil.NoSuchProcess:
        return False
    except Exception as e:
        print(f"❌ Ошибка при убийстве процесса {pid}: {e}")
        return False

def cleanup_chromium():
    """Основная функция очистки"""
    print("🔍 Ищем процессы Chromium...")
    
    processes = find_chromium_processes()
    
    if not processes:
        print("✅ Процессы Chromium не найдены")
        return
    
    print(f"📊 Найдено {len(processes)} процессов Chromium:")
    
    total_memory = 0
    for proc in processes:
        try:
            memory_mb = proc.info['memory_info'].rss / 1024 / 1024
            total_memory += memory_mb
            print(f"   PID {proc.info['pid']}: {proc.info['name']} - {memory_mb:.1f} MB")
        except:
            print(f"   PID {proc.info['pid']}: {proc.info['name']} - память недоступна")
    
    print(f"💾 Общее потребление памяти: {total_memory:.1f} MB")
    
    # Спрашиваем подтверждение
    response = input("\n❓ Убить все процессы Chromium? (y/N): ").strip().lower()
    
    if response not in ['y', 'yes', 'да']:
        print("❌ Операция отменена")
        return
    
    print("\n🧹 Убиваем процессы...")
    
    killed_count = 0
    failed_count = 0
    
    for proc in processes:
        try:
            pid = proc.info['pid']
            if kill_process_tree(pid):
                print(f"✅ Процесс {pid} убит")
                killed_count += 1
            else:
                print(f"❌ Не удалось убить процесс {pid}")
                failed_count += 1
        except Exception as e:
            print(f"❌ Ошибка при обработке процесса {proc.info['pid']}: {e}")
            failed_count += 1
    
    print(f"\n📊 Результат очистки:")
    print(f"   ✅ Убито: {killed_count}")
    print(f"   ❌ Ошибок: {failed_count}")
    
    if killed_count > 0:
        print(f"💾 Освобождено памяти: ~{total_memory:.1f} MB")

def force_cleanup():
    """Принудительная очистка без подтверждения (для автоматизации)"""
    print("🧹 Принудительная очистка процессов Chromium...")
    
    processes = find_chromium_processes()
    
    if not processes:
        print("✅ Процессы Chromium не найдены")
        return
    
    killed_count = 0
    for proc in processes:
        try:
            if kill_process_tree(proc.info['pid']):
                killed_count += 1
        except:
            pass
    
    print(f"✅ Убито процессов: {killed_count}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--force":
        force_cleanup()
    else:
        cleanup_chromium()
