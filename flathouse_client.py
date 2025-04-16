import csv

def read_csv(file_path, delimiter, selected_columns):
    rows = []
    try:
        with open(file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=delimiter)
            for row in reader:
                filtered_row = [row[col] for col in selected_columns if col in row]
                rows.append(filtered_row)
        return selected_columns, rows
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return [], []
