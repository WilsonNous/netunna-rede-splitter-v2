import os
import re

INVALID_FN_CHARS = re.compile(r'[^A-Za-z0-9._-]')

def sanitize_filename(name):
    s = INVALID_FN_CHARS.sub('_', name.strip())
    return re.sub(r'_+', '_', s)

def ensure_outfile(path_dir, filename):
    os.makedirs(path_dir, exist_ok=True)
    filename = sanitize_filename(filename)
    return os.path.join(path_dir, filename)
