import shutil
import os

def move_processed_file(source_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    dest_path = os.path.join(target_dir, os.path.basename(source_path))
    shutil.move(source_path, dest_path)
    print(f'📦 Arquivo movido para {dest_path}')
