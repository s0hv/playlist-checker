import json
import os
from typing import Dict

opt_path = 'yt_dlp_options.json'


def get_yt_dlp_options() -> Dict:
    if not os.path.exists(opt_path):
        return {}

    with open(opt_path, 'r', encoding='utf-8') as f:
        return json.load(f)
