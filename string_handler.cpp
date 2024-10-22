#ifndef file_han 

#define file_han 69


#include <fstream>
#include <filesystem>
#include "meta.h"
#include "fileHandler.cpp"
#include "cred.cpp"
#include <string>

#define FILE_LIMIT 1024*1024*10 


void closeFile(std::ofstream &outFile){
    std::ofstream *out_ptr = &outFile;
    delete out_ptr;

}

std::ofstream & get_file_to_write(int &file_no, std::string table_name){

    const std::string file_name =  get_path()+table_name  + "/string_" + std::to_string(file_no);
    std::ofstream * outFile = new std::ofstream;
        // Check if the file exists using std::filesystem
    if (std::filesystem::exists(file_name))
    {
        // If the file exists, open it with both std::ios::in and std::ios::out (read and write)
        outFile->open(file_name, std::ios::binary | std::ios::in | std::ios::out);
    }
    else
    {
        // If the file doesn't exist, open it with std::ios::out to create it
        outFile->open(file_name, std::ios::binary | std::ios::out);
    }
    int f_size = get_size(file_name );


    if (f_size > FILE_LIMIT)
    {
        closeFile(*outFile);
        file_no += 1;
        return get_file_to_write(file_no, table_name);
    }
    
    return *outFile;


}

Dbstr str_file_writer(std::ofstream &outFile, std::string str,const int file_no){
    
    Dbstr str_struct;
    outFile.seekp(0, std::ios::end);
    int offset =  outFile.tellp();
    str_struct.file = file_no;
    str_struct.offset_start = offset;
    str_struct.size = str.size();


    outFile.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    outFile.seekp(offset, std::ios::beg);    
    outFile.write(str.data(), str_struct.size);

    return str_struct;
}



#if 0


//testing
int main(int argc, char const *argv[])
{

    get_home_folder();

    int file_no = 1;
    
    std::ofstream & str_file = get_file_to_write(file_no,"kcc");
    
    Dbstr str = str_file_writer(str_file, "jith", file_no);


    closeFile(str_file);


    std::cout <<"file: " << str.file << "  offset: "<<str.offset_start <<"   size: "<< str.size << std::endl;

    


    return 0;
}

#endif

#endif