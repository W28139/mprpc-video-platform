#pragma once
#include<unordered_map>
#include<string>
// 框架读取配置文件类
class MprpcConfig
{
public:
    // 负责解析加载配置文件
    bool LoadConfigFile(const char* config_file);
    // 查询配置项信息
    std::string Load(const std::string &key);
    // 必填配置：缺失时返回 false，并把错误文本写到 error
    bool LoadRequired(const std::string& key, std::string& value, std::string& error) const;
    // 整型配置：缺失时使用默认值，存在但非法时返回默认值并记录日志
    int LoadInt(const std::string& key, int defaultValue, int minValue, int maxValue) const;
    bool HasKey(const std::string& key) const;
private:
    std::unordered_map<std::string, std::string>m_configMap;

};
