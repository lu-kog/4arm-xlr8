#pragma once

#ifndef LOGGER_H
#define LOGGER_H 0

#include <string>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <memory>
#include <mutex>
#include <chrono>
#include <ctime>



/*----------------------------Timer------------------------------------*/
#define CHRONO_NOW (std::chrono::high_resolution_clock::now())

#define TIME_DIFF(st_time, end_time) \
		((std::chrono::duration_cast<std::chrono::microseconds>(end_time-st_time).count()) / 1e6)

#define PRINT_TIME_TAKEN(msg, t) std::cout << msg << t << std::endl;
/*---------------------------------------------------------------------*/

#define LOG_DEBUG(message) Logger::getInstance().log(Logger::LogLevel::DEBUG, (message))
#define LOG_INFO(message) Logger::getInstance().log(Logger::LogLevel::INFO, (message))
#define LOG_WARN(message) Logger::getInstance().log(Logger::LogLevel::WARNING, (message))
#define LOG_ERROR(message) Logger::getInstance().log(Logger::LogLevel::ERROR, (message))


/*---------------------------------------------------------------------*/

struct Logger {

    enum LogLevel {
        DEBUG,
        INFO,
        WARNING,
        ERROR
    };

    static Logger& getInstance(); // Singleton instance

    void setLogLevel(LogLevel level);
    void setLogFile(const std::string& filename);

    void log(LogLevel level, const std::string& message);
    void save_logger();

    LogLevel currentLogLevel;
    std::ofstream logFile;
    std::mutex logMutex; // For thread-safe logging

    Logger();  
    ~Logger(); 
    Logger(const Logger&) = delete;   // to achieve singleton
    Logger& operator=(const Logger&) = delete;

    std::string getTimestamp();               
    std::string logLevelToString(LogLevel level); // Convert log level to string
};

#endif // LOGGER_H
