#!/usr/bin/env python3
import json
import os
import sys


def clean_specs_file(filename: str):
    if not os.path.exists(filename):
        print(f"File {filename} không tồn tại")
        return

    with open(filename, "r", encoding="utf-8") as f:
        try:
            listings = json.load(f)
        except Exception as e:
            print(f"Lỗi đọc file JSON: {e}")
            return

    if not isinstance(listings, list):
        print("File JSON phải chứa một mảng (list) các dict")
        return

    for item in listings:
        if not isinstance(item, dict):
            continue
        for key in ["Nhiên liệu", "Hộp số"]:
            if key in item and isinstance(item[key], str):
                val = item[key]
                if val.startswith(f"{key}:"):
                    item[key] = val.split(":", 1)[1].strip()

    # ghi đè lại file
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)

    print(f"Đã làm sạch dữ liệu và ghi lại vào {filename}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Cách dùng: python clean_specs.py <file.json>")
    else:
        clean_specs_file(sys.argv[1])
        clean_specs_file(sys.argv[1])
