import os
import json
from deepdiff import DeepDiff

# Function to load JSON files from a directory
def load_json_files(directory):
    json_files = {}
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('tx_json.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    json_files[file_path] = json.load(f)
    return json_files

# Function to compare JSON files and classify differences
def compare_json_files(json_files):
    differences = {}
    file_paths = list(json_files.keys())
    for i in range(len(file_paths)):
        for j in range(i + 1, len(file_paths)):
            file1 = file_paths[i]
            file2 = file_paths[j]
            diff = DeepDiff(json_files[file1], json_files[file2], ignore_order=True)
            if diff:
                differences[(file1, file2)] = diff
    return differences

# Load JSON files from the directories
directory1 = '/home/rese/Documents/rese/xrplWalletTracker/test_tx'
# directory2 = '/path/to/second/directory'
json_files1 = load_json_files(directory1)
# json_files2 = load_json_files(directory2)

# Combine JSON files from both directories
# all_json_files = {**json_files1, **json_files2}

# Compare JSON files and classify differences
differences = compare_json_files(json_files1)

# Print the differences
for file_pair, diff in differences.items():
    print(f"Differences between {file_pair[0]} and {file_pair[1]}:")
    print(diff)
    print("\n")