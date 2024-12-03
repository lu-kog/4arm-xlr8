#pragma once
#ifndef _DB_STRING_H 


#define _DB_STRING_H 0

#include <iostream>
#include <fstream>
#include <filesystem>
#include "Utilities.h" // dependancy getpath, getsize



#define FILE_LIMIT 1024*1024*1024  // 1gb limit for a string file


struct Dbstr{
    int file;
    int offset_start;
    int size;

    /*
     * String dosen't have any comparative opration 
     */
    bool operator>(const Dbstr& other) const {
        return false;
    }
    bool operator<(const Dbstr& other) const {
        return false;
    }
    bool operator==(const Dbstr& other) const {
        return false;
    } 

};

std::ostream &operator<<(std::ostream & os, Dbstr str);



struct{
    int file_no_for_str = 0;
    std::ifstream file_for_str;
}file_to_make_non_rep_string;

void get_file_to_write(int &file_no, std::string table_name , std::ofstream &outFile);
Dbstr str_file_writer(std::ofstream &outFile, std::string str,const int file_no);
void string_write_file_size_check(int &file_no, std::string table_name, std::ofstream &str_file_obj);
std::string str_file_reader(const std::string& table_name, const Dbstr& str_info);
Dbstr insert_new_string(const std::string &table_name, std::string str);

#endif