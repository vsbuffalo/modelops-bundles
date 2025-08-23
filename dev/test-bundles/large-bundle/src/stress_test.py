#!/usr/bin/env python3
"""
Stress test script for large bundle testing.
"""
import os
import time

def process_large_files():
    """Process large files to test streaming and performance."""
    print("=== Large Bundle Stress Test ===")
    
    # Check for large data files
    data_dir = "data"
    models_dir = "models"
    
    total_size = 0
    file_count = 0
    
    for root, dirs, files in os.walk("."):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                size = os.path.getsize(file_path)
                total_size += size
                file_count += 1
            except OSError:
                pass
    
    print(f"Bundle contains {file_count} files")
    print(f"Total size: {total_size / 1024 / 1024:.2f} MB")
    
    # Simulate processing
    print("Simulating large file processing...")
    time.sleep(2)
    
    return {
        "files": file_count,
        "total_size_mb": round(total_size / 1024 / 1024, 2),
        "status": "success"
    }

if __name__ == "__main__":
    result = process_large_files()
    print(f"Stress test result: {result}")