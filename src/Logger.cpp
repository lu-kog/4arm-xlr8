#include "../include/Logger.h"


// Constructor
Logger::Logger() 
    : currentLogLevel(LogLevel::INFO){}

// Destructor
Logger::~Logger() {
    if (logFile.is_open()) {
        logFile.close();
    }
}

// Singleton instance
Logger& Logger::getInstance() {
    static Logger instance;
    return instance;
}

// Set log level
void Logger::setLogLevel(LogLevel level) {
    currentLogLevel = level;
}

// Set log file output
void Logger::setLogFile(const std::string& filename) {
    if (logFile.is_open()) {
        logFile.close();
    }
    if (std::filesystem::exists(filename))
    {
        logFile.open(filename, std::ios::out | std::ios::app); 
    }else{
        logFile.open(filename, std::ios::out);  // create and open
    }
    
    if (!logFile) {
        throw std::runtime_error("Failed to open log file: " + filename);
    }
}


// Log a message
void Logger::log(LogLevel level, const std::string& message) {
    if (level < currentLogLevel) {
        return;
    }

    std::string timestamp = getTimestamp();
    std::string logLevelStr = logLevelToString(level);
    std::string formattedMessage = "[" + timestamp + "] [" + logLevelStr + "] " + message;

    std::lock_guard<std::mutex> lock(logMutex);

    // Log to file if open
    if (logFile.is_open()) {
        logFile << formattedMessage << std::endl;
    }

}

// Get current timestamp
std::string Logger::getTimestamp() {
    auto in_time_t = std::chrono::system_clock::to_time_t(CHRONO_NOW);
    std::ostringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

// Convert log level to string
std::string Logger::logLevelToString(LogLevel level) {
    switch (level) {
        case LogLevel::INFO: return "INFO";
        case LogLevel::WARNING: return "WARNING";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::DEBUG: return "DEBUG";
        default: return "UNKNOWN";
    }
}

