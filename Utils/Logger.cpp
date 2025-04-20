//
// Created by Riju Mukherjee on 02-02-2025.
//

#include "../headers/Logger.h"
#include "../headers/constants.h"
#include <iostream>
#include <filesystem>
Utils::Logger::Logger()
{
    try
    {
        std::filesystem::create_directories("C:\\ndb\\logs");
        file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(R"(C:\ndb\logs\logs.txt)");
        file_logger = std::make_shared<spdlog::logger>("FileLogger",file_sink);
        console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        // Set the color for different log levels
        console_sink->set_color(spdlog::level::info, 2047); //cyan
        console_sink->set_color(spdlog::level::warn, 65504); //yellow
        console_sink->set_color(spdlog::level::err, 63488); // red
        console_sink->set_color(spdlog::level::critical, 9999);
        con_logger = std::make_shared<spdlog::logger>("ConsoleLogger",console_sink);
        spdlog::register_logger(file_logger);
        spdlog::register_logger(con_logger);
        spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");
    }
    catch (const spdlog::spdlog_ex::spdlog_ex::exception ex)
    {
        std::cerr << "Logger initialization failed: " << ex.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }

}
Utils::Logger* Utils::Logger::getInstance()
{
    static Logger* instance = nullptr;
    static std::mutex mtx;
    mtx.lock();
    if (instance == nullptr)
    {
        instance = new Logger();
    }
    mtx.unlock();
    return instance;
}

void Utils::Logger::logError(std::initializer_list<const std::string> msgs) const
{
    std::string final_msg;
    for (const auto& msg : msgs)
    {
        final_msg += msg+" ";
    }
    con_logger->error(final_msg);
    file_logger->error(final_msg);
}

void Utils::Logger::logWarn(std::initializer_list<const std::string> msgs) const
{
    std::string final_msg;
    for (const auto& msg : msgs)
    {
        final_msg += msg+" ";
    }
    con_logger->warn(final_msg);
    file_logger->warn(final_msg);
}
void Utils::Logger::logInfo(std::initializer_list<const std::string> msgs) const
{
    std::string final_msg;
    for (const auto& msg : msgs)
    {
        final_msg += msg+" ";
    }
    con_logger->info(final_msg);
    file_logger->info(final_msg);
}
void Utils::Logger::logCritical(std::initializer_list<const std::string> msgs) const
{
    std::string final_msg;
    for (const auto& msg : msgs)
    {
        final_msg += msg+" ";
    }
    con_logger->critical(final_msg);
    file_logger->critical(final_msg);
    throw std::runtime_error("Critical Error ... Exiting");
}
