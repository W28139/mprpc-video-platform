#include"mprpcconfig.h"
#include"wevix_muduo/AsyncLogger.h"
#include<cstdlib>
#include<cerrno>
#include<climits>
#include<string>

// 辅助函数：去掉字符串前后的空格
void Trim(std::string &src_buf)
{
    // 去掉字符串前面多余的空格
    size_t idx = src_buf.find_first_not_of(" \t");
    if (idx == std::string::npos)
    {
        // 整行都是空白字符时，直接变成空串，后续按空行处理。
        src_buf.clear();
        return;
    }

    if (idx != std::string::npos)
    {
        // 说明字符串前面有空格
        src_buf = src_buf.substr(idx, src_buf.size() - idx);
    }

    // 去掉字符串后面多余的空格
    idx = src_buf.find_last_not_of(" \t");
    if (idx != std::string::npos)
    {
        // 说明字符串后面有空格
        src_buf = src_buf.substr(0, idx + 1);
    }
}

bool MprpcConfig::LoadConfigFile(const char* config_file)
{
    FILE *pf = fopen(config_file, "r");
    if (nullptr == pf)
    {
        LOG_ERROR("%s is not exist", config_file);
        return false;
    }
    m_configMap.clear();

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
        size_t idx = src_buf.find('=');
        if (idx == std::string::npos)
        {
            // 配置项不合法
            LOG_WARN("invalid config line: %s", src_buf.c_str());
            continue;
        }

        std::string key = src_buf.substr(0, idx);
        Trim(key); // 处理 key 后面的空格，如 "rpcserverip = 127.0.0.1"

        std::string value = src_buf.substr(idx + 1);
        Trim(value); // 处理 value 前面的空格

        // 去掉行内注释（" #" 及之后的内容），如 "8080 # 端口号" → "8080"
        // 仅在 # 前有空格时才视为注释，避免截断 URL fragment 等含 # 的合法值
        size_t comment_pos = value.find(" #");
        if (comment_pos != std::string::npos)
        {
            value = value.substr(0, comment_pos);
            Trim(value); // 去掉注释前的尾部空格
        }

        // 5. 存储到 map 容器中
        m_configMap[key] = value;
    }

    fclose(pf);
    return true;
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

bool MprpcConfig::LoadRequired(const std::string& key,
                               std::string& value,
                               std::string& error) const
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        error = "required config key missing or empty: " + key;
        return false;
    }

    value = it->second;
    return true;
}

int MprpcConfig::LoadInt(const std::string& key,
                         int defaultValue,
                         int minValue,
                         int maxValue) const
{
    auto it = m_configMap.find(key);
    if (it == m_configMap.end() || it->second.empty())
    {
        return defaultValue;
    }

    errno = 0;
    char* end = nullptr;
    long value = std::strtol(it->second.c_str(), &end, 10);
    if (errno != 0 || end == it->second.c_str() || *end != '\0' ||
        value < minValue || value > maxValue)
    {
        LOG_WARN("invalid int config: key=%s, value=%s, use default=%d",
                 key.c_str(), it->second.c_str(), defaultValue);
        return defaultValue;
    }

    return static_cast<int>(value);
}

bool MprpcConfig::HasKey(const std::string& key) const
{
    return m_configMap.find(key) != m_configMap.end();
}
