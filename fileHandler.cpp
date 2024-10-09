#ifndef _fileHandler
#define _fileHandler 1

#include <iostream>
#include <fstream>


void readBinaryFile(const std::string &filename, char *buffer, long long size, int offset)
{
    try
    {
        std::ifstream inFile(filename, std::ios::binary);

        // Enable exceptions for failure
        inFile.exceptions(std::ifstream::failbit | std::ifstream::badbit);

        // Move the file pointer to the specified offset
        inFile.seekg(offset, std::ios::beg);

        // Read 'size' bytes into the buffer
        inFile.read(buffer, size);

        inFile.close();
    }
    catch (const std::ios_base::failure &e)
    {
        std::cerr << "Error reading from file: " << e.what() << std::endl;
    }
}

void writeRecords(const std::string &filename, const char *buffer, long long size, long long offSet)
{
    try
    {
        
        std::ofstream outFile;
        
        // Check if the file exists using std::filesystem
        if (std::filesystem::exists(filename))
        {
            // If the file exists, open it with both std::ios::in and std::ios::out (read and write)
            outFile.open(filename, std::ios::binary | std::ios::in | std::ios::out);
        }
        else
        {
            // If the file doesn't exist, open it with std::ios::out to create it
            outFile.open(filename, std::ios::binary | std::ios::out);
        }




        outFile.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        outFile.seekp(offSet, std::ios::beg);
        outFile.write(buffer, size);
        outFile.close();
    }
    catch (const std::ios_base::failure &e)
    {
        std::cerr << "Error writing to a file: " << e.what() << '\n';
    }
}




int get_size(std::string file_name){
    std::ifstream in_file(file_name, std::ios::binary);
   in_file.seekg(0, std::ios::end);
   int file_size = in_file.tellg();
   return file_size;
}



int main(int argc, char const *argv[])
{
    
    char buffer[] = "Krisi";
    std::cout << (int *) buffer << std::endl;
    
    writeRecords("/home/gokul-zstk330/Downloads/jith.bin",buffer, 5, 0);

    return 0;
}

#endif