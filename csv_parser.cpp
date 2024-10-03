#pragma once

#ifndef _CSVParser
#define _CSVParser 0

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos)
        return str;
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

void parseCSV(const std::ifstream &file, schema & tableSchema) {

    
    std::string line;
    
    // Read CSV line by line
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string value;
        bool insideQuotes = false;
        std::vector<std::string> parsedValues;
        std::string temp;

        // Parse the line, handling quoted strings with commas
        // Temporary implementation
        //Bug: Quotation inside the string and comma. "Hi, "How are you?, Fine.",

        for (char c : line) {
            if (c == '"') {
                insideQuotes = !insideQuotes; 
            }
            else if (c == ',' && !insideQuotes) {
                // If not inside quotes, this is a delimiter
                parsedValues.push_back(trim(temp));
                temp.clear();
            }
            else {
                temp += c; // Add character to current token
            }
        }
        // the last value after the loop
        parsedValues.push_back(trim(temp));

        push_to_vector(parsedValues, tableSchema); //

    }
}

int main() {
    // Vectors to store the parsed column data
    std::vector<int> intColumn;
    std::vector<float> floatColumn;
    std::vector<std::string> stringColumn;

    // Parse the CSV file
    parseCSV("data.csv", intColumn, floatColumn, stringColumn);

    // Print the parsed data for verification
    std::cout << "Parsed CSV Data:" << std::endl;
    for (size_t i = 0; i < intColumn.size(); ++i) {
        std::cout << intColumn[i] << ", " << floatColumn[i] << ", " << stringColumn[i] << std::endl;
    }

    return 0;
}


void push_to_vector(std::vector *);

#endif