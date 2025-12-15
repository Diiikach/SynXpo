#pragma once

#include <iostream>
#include <string>
#include <mutex>

namespace synxpo {

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    NONE
};

class Logger {
public:
    static Logger& Instance() {
        static Logger instance;
        return instance;
    }

    void SetLevel(LogLevel level) {
        level_ = level;
    }

    LogLevel GetLevel() const {
        return level_;
    }

    void Debug(const std::string& message) {
        Log(LogLevel::DEBUG, "[DEBUG] ", message);
    }

    void Info(const std::string& message) {
        Log(LogLevel::INFO, "[INFO] ", message);
    }

    void Warning(const std::string& message) {
        Log(LogLevel::WARNING, "[WARNING] ", message);
    }

    void Error(const std::string& message) {
        Log(LogLevel::ERROR, "[ERROR] ", message);
    }

private:
    Logger() : level_(LogLevel::INFO) {}
    
    void Log(LogLevel msg_level, const std::string& prefix, const std::string& message) {
        if (msg_level < level_) {
            return;
        }
        
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << prefix << message << std::endl;
    }

    LogLevel level_;
    std::mutex mutex_;
};

// Convenience macros
#define LOG_DEBUG(msg) synxpo::Logger::Instance().Debug(msg)
#define LOG_INFO(msg) synxpo::Logger::Instance().Info(msg)
#define LOG_WARNING(msg) synxpo::Logger::Instance().Warning(msg)
#define LOG_ERROR(msg) synxpo::Logger::Instance().Error(msg)

}  // namespace synxpo
