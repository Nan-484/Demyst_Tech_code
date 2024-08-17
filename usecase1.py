import os
import json
import csv

# Global variables for file paths
JSON_FILE_PATH = r"D:\Snowflake\Sample_Files\specs.json"
FIXED_WIDTH_OUTPUT_PATH = r"D:\Snowflake\Sample_Files\fixed_width_output.txt"
CSV_OUTPUT_PATH = r"D:\Snowflake\Sample_Files\output.csv"


def load_config(json_file):
    ###Load configuration from a JSON file.
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


def write_fixed_width_file(config, output_file, data):
    ###Write data to a fixed-width file based on the configuration.
    column_names = config["ColumnNames"]
    offsets = list(map(int, config["Offsets"]))
    fixed_width_encoding = config["FixedWidthEncoding"]
    include_header = config["IncludeHeader"].lower() == "true"

    with open(output_file, "w", encoding=fixed_width_encoding) as fw_file:
        if include_header:
            header = "".join(
                f"{name:<{length}}" for name, length in zip(column_names, offsets)
            )
            fw_file.write(header + "\n")

        for row in data:
            fixed_width_line = "".join(
                f"{str(field)[:length]:<{length}}"
                for field, length in zip(row, offsets)
            )
            fw_file.write(fixed_width_line + "\n")

    print(f"fixed width file created successfully at: {os.path.abspath(output_file)}")


def read_fixed_width_file(config, fixed_width_file):
    ###Read and parse a fixed-width file based on the configuration
    offsets = list(map(int, config["Offsets"]))
    fixed_width_encoding = config["FixedWidthEncoding"]

    field_positions = []
    current_position = 0
    for offset in offsets:
        field_positions.append((current_position, current_position + offset))
        current_position += offset

    with open(fixed_width_file, "r", encoding=fixed_width_encoding) as fw_file:
        lines = fw_file.readlines()

    if config["IncludeHeader"].lower() == "true":
        lines = lines[1:]  # Skip header line

    parsed_data = []
    for line in lines:
        parsed_fields = [line[start:end].strip() for start, end in field_positions]
        parsed_data.append(parsed_fields)

    return parsed_data


def write_csv_file(config, parsed_data, output_csv_file):
    ###Write parsed data to a CSV file
    delimited_encoding = config["DelimitedEncoding"]

    with open(
        output_csv_file, "w", newline="", encoding=delimited_encoding
    ) as csv_file:
        csv_writer = csv.writer(csv_file)
        if config["IncludeHeader"].lower() == "true":
            csv_writer.writerow(config["ColumnNames"])
        csv_writer.writerows(parsed_data)

    print(f"CSV file created successfully at: {os.path.abspath(output_csv_file)}")


if __name__ == "__main__":
    config = load_config(JSON_FILE_PATH)

    ###Sample data to be written to the fixed-width file
    sample_data = [
        [
            "Amit",
            "Sharma",
            "32",
            "M",
            "15 MG Road",
            "Bangalore",
            "KA",
            "560001",
            "India",
            "Customer",
        ],
        [
            "Priya",
            "Raj",
            "29",
            "F",
            "22 Sardar St",
            "Ahmedabad",
            "GJ",
            "380009",
            "India",
            "Employee",
        ],
        [
            "Rahul",
            "Singh",
            "45",
            "M",
            "7 Park Lane",
            "Mumbai",
            "MH",
            "400001",
            "India",
            "Supplier",
        ],
    ]

    ###Create fixed-width file
    write_fixed_width_file(config, FIXED_WIDTH_OUTPUT_PATH, sample_data)

    ###Parse fixed-width file and generate CSV
    parsed_data = read_fixed_width_file(config, FIXED_WIDTH_OUTPUT_PATH)
    write_csv_file(config, parsed_data, CSV_OUTPUT_PATH)
