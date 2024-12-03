#pragma once

#ifndef _UTILITIES_H
#define _UTILITIES_H 0

#include <iostream>
#include <fstream>
#include <filesystem>
#include <vector>
#include <set>
#include <algorithm>
#include <iostream>
#include <chrono>

#define DBINT 1
#define DBCHAR 2
#define DBLONG 3
#define DBDOUBLE 4
#define DBSTRING 5
#define DBFLOAT 6

#define RECORDS_LIMIT 100
#define BLOCK_LIMIT 100

// File IO functions
static std::string path;
void get_home_folder();
bool create_folder(std::string name);
bool delete_folder(const std::string& folder_path);
std::string get_path();
namespace fs = std::filesystem;
std::string get_file_path (const std::string& table_name,  const std::string &col_name);
void readBinaryFile(const std::string &filename, char *buffer, long long size, int offset);
void readBinaryFile(char *buffer, long long size, int offset, std::ifstream &inFile);
void writeBinaryFile(const std::string &filename, const char *buffer, long long size, long long offSet);
int get_size(std::string file_name);

void Roll_Back(const std::string& table_name);
void backup_table_data(const std::string& table_name);

// Filter Helper Functions
typedef std::vector<int>* RowID_vector;
RowID_vector mergeAndRemoveDuplicates(RowID_vector vec1, RowID_vector vec2);


/*----------------------------Timer------------------------------------*/




#define CHRONO_NOW (std::chrono::high_resolution_clock::now())

#define TIME_DIFF(st_time, end_time) \
		((std::chrono::duration_cast<std::chrono::microseconds>(end_time-st_time).count()) / 1e6)

#define PRINT_TIME_TAKEN(msg, t) std::cout << msg << t << std::endl;
/*---------------------------------------------------------------------*/




/*-----------------------------------------------------------------------*/




#endif