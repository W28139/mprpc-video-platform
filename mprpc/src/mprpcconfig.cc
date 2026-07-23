#include"mprpcconfig.h"
#include"wevix_muduo/AsyncLogger.h"
#include<string>

// 辅助函数：去掉字符串前后的空格
void Trim(std::string &src_buf)
{
    // 去掉字符串前面多余的空格
    int idx = src_buf.find_first_not_of(' ');
    if (idx != -1)
    {
        // 说明字符串前面有空格
        src_buf = src_buf.substr(idx, src_buf.size() - idx);
    }

    // 去掉字符串后面多余的空格
    idx = src_buf.find_last_not_of(' ');
    if (idx != -1)
    {
        // 说明字符串后面有空格
        src_buf = src_buf.substr(0, idx + 1);
    }
}

void MprpcConfig::LoadConfigFile(const char* config_file)
{
    FILE *pf = fopen(config_file, "r");
    if (nullptr == pf)
    {
        LOG_FATAL("%s is not exist", config_file);
        exit(EXIT_FAILURE);
    }

    // 开始读取配置文件
    while (!feof(pf))
    {
        char buf[512] = {0};
        if (fgets(buf, 512, pf) == nullptr) continue;

        std::string src_buf(buf);
        
        // 1. 去掉换行符（fgets会保留\n）
        size_t end_pos = src_buf.find_last_not_of("\r\n");
        if (end_pos != std::string::npos) {
            src_buf = src_buf.substr(0, end_pos + 1);
        }

        // 2. 去掉前后空格
        Trim(src_buf);

        // 3. 判断是否是注释或空行
        if (src_buf.empty() || src_buf[0] == '#')
        {
            continue;
        }

        // 4. 解析配置项 (key=value)
        int idx = src_buf.find('=');
        if (idx == -1)
        {
            // 配置项不合法
            LOG_WARN("invalid config line: %s", src_buf.c_str());
            continue;
        }

        std::string key = src_buf.substr(0, idx);
        Trim(key); // 处理 key 后面的空格，如 "rpcserverip = 127.0.0.1"

        std::string value = src_buf.substr(idx + 1);
        Trim(value); // 处理 value 前面的空格

        // 5. 存储到 map 容器中
        m_configMap[key] = value;
    }

    fclose(pf);
}


std::string MprpcConfig::Load(const std::string &key)
{
    // return m_configMap[key]; 如果这样写，如果key不存在，他会自己向map里增加内容
    auto it = m_configMap.find(key);
    if(it==m_configMap.end())
    {
        LOG_WARN("config key not found: %s", key.c_str());
        return "";
    }
    return it->second; 
}