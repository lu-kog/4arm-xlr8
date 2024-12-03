

#ifndef _CSVParser
#define _CSVParser 0

#define buffer_limit 100*1024*1024    //10 MB

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos)  // return 18446744073709551615UL = -1
        return str;
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

std::vector<std::string> * parseCSV(std::ifstream &file) {

    std::vector<std::string> * parsed_data = new std::vector<std::string>;

    unsigned int current_buffer = 0;
    std::string line;
    bool limitReached = false;

    int line_count=0;

    // Read CSV line by line
    while ((!limitReached) && std::getline(file, line)) {
        line_count++;
        bool insideQuotes = false;
        std::string temp;
        current_buffer += line.size();

        if (current_buffer >= buffer_limit)
        {
            limitReached = true;
        }
        

        // Temporary implementation
        // Bug: Quotation inside the string and comma. "Hi, "How are you?, Fine.", skipping insided double quotes. 

        for (char c : line) {
            if (c == '"') {
                insideQuotes = !insideQuotes; 
            }
            else if (c == ',' && !insideQuotes) {
                // If not inside quotes, this is a delimiter
                parsed_data->push_back(trim(temp));
                temp.clear();
            }
            else {
                temp += c; // Add character to current token
            }
        }
        // the last value after the loop
        parsed_data->push_back(trim(temp));
        
    }

    if (parsed_data->size()==0)
    {
        return nullptr;
    }else{
        return parsed_data;
    }
    
}


#endif