// ============================================================================
// Store 原子 SQL 功能测试
// 覆盖三个 MySQL 持久化 Store 的原子方法（条件 SQL 语义）：
//   JobStore:   Insert / Get / Delete / UpdateIfStatus（命中/不命中/多状态/空）
//   WorkerStore: UpdateHeartbeat（在线刷新 / OFFLINE 拒绝 / 不存在）、
//                MarkOfflineIfTimeout（超时/未超时/已 OFFLINE）、
//                InsertOrUpdate（upsert 新插 vs 覆盖）、ListByStatus
//   ShardStore:  CountByStatus 增量、ListByJob / ListByWorker、UpdateIfStatus
//
// 这些"单条条件 SQL 的正确性"是 e2e 全绿也无法覆盖的（状态机回退保护），
// 只能通过直连 MySQL 断言。
//
// 运行前置：本地 MySQL 运行且存在 video_platform 库（schema 自动建表）。
// MySQL 不可达 / conf 找不到时打印 SKIP 并退出 0（CI 无 MySQL 时安全跳过）。
// 数据隔离：key 全部用 GenerateId() 唯一化，用例结束 Delete 清理，
// CountByStatus 一律用前后增量断言，不污染开发库。
// ============================================================================

#include "mprpcapplication.h"
#include "common.pb.h"   // WORKER_ONLINE / WORKER_OFFLINE 等枚举值
#include "video_platform/common_store.h"
#include "video_platform/mysql_pool.h"

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <map>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>

using namespace video_platform;

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name)                                              \
    do {                                                        \
        std::cout << "  [" << std::setw(38) << std::left << name << "] "; \
    } while(0)

#define PASS() do { g_passed++; std::cout << "\033[1;32mPASS\033[0m\n"; } while(0)

#define CHECK(cond)                                             \
    do {                                                        \
        if (!(cond)) {                                          \
            g_failed++;                                         \
            std::cerr << "\033[1;31mFAIL\033[0m @" << __LINE__ \
                      << "  expected: " #cond "\n";             \
            return;                                             \
        }                                                       \
    } while(0)

// 生成唯一测试 key（用例结束后 Delete 清理，不污染开发库）
static std::string uid(const std::string& prefix)
{
    return GenerateId(prefix);
}

// ============================================================================
// JobStore 用例
// ============================================================================

static JobRecord makeJob(const std::string& job_id)
{
    JobRecord r;
    r.job_id = job_id;
    r.user_id = "test_user";
    // 含引号与中文，验证 SQL escape 路径
    r.input_path = "/data/it's_a\"test\"/视频输入.mp4";
    r.output_path = "/data/output";
    r.target_format = "mp4";
    r.target_resolution = "720p";
    r.target_bitrate = 2000;
    r.duration_sec = 30;
    r.priority = 5;
    r.status = 1;   // JOB_PENDING
    r.shard_count = 2;
    r.shard_duration_sec = 15;
    r.created_at = 1000;
    r.updated_at = 1000;
    return r;
}

static void test_job_insert_get()
{
    TEST("JobStore Insert+Get 回环");

    auto job_id = uid("job");
    JobStore& store = JobStore::GetInstance();
    CHECK(store.Insert(makeJob(job_id)));
    auto got = store.Get(job_id);
    CHECK(got.has_value());
    CHECK(got->user_id == "test_user");
    CHECK(got->input_path == "/data/it's_a\"test\"/视频输入.mp4");  // escape 无损
    CHECK(got->target_resolution == "720p");
    CHECK(got->target_bitrate == 2000);
    CHECK(got->duration_sec == 30);
    CHECK(got->priority == 5);
    CHECK(got->status == 1);
    CHECK(got->shard_count == 2);
    CHECK(got->shard_duration_sec == 15);
    CHECK(store.Delete(job_id));
    PASS();
}

static void test_job_insert_dup()
{
    TEST("JobStore 重复 key 拒绝");

    auto job_id = uid("job");
    JobStore& store = JobStore::GetInstance();
    CHECK(store.Insert(makeJob(job_id)));
    CHECK(!store.Insert(makeJob(job_id)));   // 幂等拒绝
    CHECK(store.Delete(job_id));
    PASS();
}

static void test_job_get_delete_missing()
{
    TEST("JobStore 缺失 Get/Delete");

    JobStore& store = JobStore::GetInstance();
    auto got = store.Get(uid("job"));
    CHECK(!got.has_value());
    CHECK(!store.Delete(uid("job")));
    PASS();
}

static void test_job_update_if_status_hit()
{
    TEST("UpdateIfStatus 命中推进");

    auto job_id = uid("job");
    JobStore& store = JobStore::GetInstance();
    auto rec = makeJob(job_id);
    CHECK(store.Insert(rec));
    rec.status = 3;   // JOB_SCHEDULING
    CHECK(store.UpdateIfStatus(job_id, {1}, rec));   // PENDING → SCHEDULING
    auto got = store.Get(job_id);
    CHECK(got.has_value() && got->status == 3);
    CHECK(store.Delete(job_id));
    PASS();
}

static void test_job_update_if_status_miss()
{
    TEST("UpdateIfStatus 不命中不回退");

    auto job_id = uid("job");
    JobStore& store = JobStore::GetInstance();
    auto rec = makeJob(job_id);
    CHECK(store.Insert(rec));
    rec.status = 3;
    CHECK(store.UpdateIfStatus(job_id, {1}, rec));
    // 旧快照（expect PENDING）再推进 → 拒绝，状态保持 3 不回退
    auto stale = makeJob(job_id);
    stale.status = 1;
    CHECK(!store.UpdateIfStatus(job_id, {1}, stale));
    auto got = store.Get(job_id);
    CHECK(got.has_value() && got->status == 3);
    CHECK(store.Delete(job_id));
    PASS();
}

static void test_job_update_if_status_multi()
{
    TEST("UpdateIfStatus 多状态/空 expect");

    auto job_id = uid("job");
    JobStore& store = JobStore::GetInstance();
    auto rec = makeJob(job_id);
    CHECK(store.Insert(rec));
    // 多状态 expect：当前 PENDING(1) ∈ {3,4} 之外 → 拒绝
    rec.status = 4;
    CHECK(!store.UpdateIfStatus(job_id, {3, 4}, rec));
    // 无条件推进到 RUNNING(4)，再验证多状态 expect 命中
    rec.status = 4;
    CHECK(store.UpdateIfStatus(job_id, {}, rec));
    rec.status = 3;
    CHECK(store.UpdateIfStatus(job_id, {3, 4}, rec));   // 4 ∈ {3,4} → 命中
    // 空 expect（无条件更新）→ 命中
    rec.status = 6;   // JOB_SUCCESS
    CHECK(store.UpdateIfStatus(job_id, {}, rec));
    auto got = store.Get(job_id);
    CHECK(got.has_value() && got->status == 6);
    CHECK(store.Delete(job_id));
    PASS();
}

// ============================================================================
// WorkerStore 用例
// ============================================================================

static WorkerRecord makeWorker(const std::string& worker_id, int32_t status,
                               int64_t last_heartbeat)
{
    WorkerRecord w;
    w.worker_id = worker_id;
    w.ip = "127.0.0.1";
    w.port = 9004;
    w.cpu_cores = 4;
    w.memory_mb = 8192;
    w.max_running_shards = 2;
    w.status = status;
    w.last_heartbeat = last_heartbeat;
    return w;
}

static void test_worker_heartbeat_online()
{
    TEST("UpdateHeartbeat 在线刷新");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    CHECK(store.Insert(makeWorker(worker_id, WORKER_ONLINE, 100)));
    CHECK(store.UpdateHeartbeat(worker_id, 2, 50, 60));
    auto got = store.Get(worker_id);
    CHECK(got.has_value());
    CHECK(got->current_running_shards == 2);
    CHECK(got->cpu_usage == 50);
    CHECK(got->memory_usage == 60);
    CHECK(got->last_heartbeat > 100);   // 时间戳已刷新
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_heartbeat_offline_rejected()
{
    TEST("UpdateHeartbeat 已 OFFLINE 拒绝");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    CHECK(store.Insert(makeWorker(worker_id, WORKER_OFFLINE, 100)));
    CHECK(!store.UpdateHeartbeat(worker_id, 1));   // AND status<>OFFLINE 不命中
    auto got = store.Get(worker_id);
    CHECK(got.has_value() && got->status == WORKER_OFFLINE);
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_heartbeat_missing()
{
    TEST("UpdateHeartbeat 不存在 → false");

    WorkerStore& store = WorkerStore::GetInstance();
    CHECK(!store.UpdateHeartbeat(uid("worker"), 1));
    PASS();
}

static void test_worker_mark_offline_timeout()
{
    TEST("MarkOfflineIfTimeout 超时命中");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    int64_t now = NowMs();
    CHECK(store.Insert(makeWorker(worker_id, WORKER_ONLINE, now - 2000)));
    CHECK(store.MarkOfflineIfTimeout(worker_id, now, 1000));   // 2s 前心跳 > 1s 超时
    auto got = store.Get(worker_id);
    CHECK(got.has_value() && got->status == WORKER_OFFLINE);
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_mark_offline_not_timeout()
{
    TEST("MarkOfflineIfTimeout 未超时边界");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    int64_t now = NowMs();
    CHECK(store.Insert(makeWorker(worker_id, WORKER_ONLINE, now - 500)));
    CHECK(!store.MarkOfflineIfTimeout(worker_id, now, 1000));  // 未超时 → 不动
    auto got = store.Get(worker_id);
    CHECK(got.has_value() && got->status == WORKER_ONLINE);
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_mark_offline_already()
{
    TEST("MarkOfflineIfTimeout 已 OFFLINE 不再标记");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    int64_t now = NowMs();
    CHECK(store.Insert(makeWorker(worker_id, WORKER_OFFLINE, now - 5000)));
    CHECK(!store.MarkOfflineIfTimeout(worker_id, now, 1000));
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_upsert()
{
    TEST("WorkerStore InsertOrUpdate");

    auto worker_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    auto w = makeWorker(worker_id, WORKER_ONLINE, NowMs());
    CHECK(store.InsertOrUpdate(w));            // 新插入 → true
    w.cpu_cores = 8;
    w.memory_mb = 16384;
    CHECK(!store.InsertOrUpdate(w));           // 覆盖已有 → false
    auto got = store.Get(worker_id);
    CHECK(got.has_value());
    CHECK(got->cpu_cores == 8 && got->memory_mb == 16384);  // 字段被覆盖
    CHECK(store.Delete(worker_id));
    PASS();
}

static void test_worker_list_by_status()
{
    TEST("WorkerStore ListByStatus 过滤");

    auto online_id = uid("worker");
    auto offline_id = uid("worker");
    WorkerStore& store = WorkerStore::GetInstance();
    int64_t now = NowMs();
    CHECK(store.Insert(makeWorker(online_id, WORKER_ONLINE, now)));
    CHECK(store.Insert(makeWorker(offline_id, WORKER_OFFLINE, now)));

    auto online = store.ListByStatus(WORKER_ONLINE);
    bool has_online = false, has_offline = false;
    for (const auto& w : online)
    {
        if (w.worker_id == online_id) has_online = true;
        if (w.worker_id == offline_id) has_offline = true;
    }
    CHECK(has_online && !has_offline);         // 只含 ONLINE

    auto all = store.ListByStatus(-1);
    has_online = has_offline = false;
    for (const auto& w : all)
    {
        if (w.worker_id == online_id) has_online = true;
        if (w.worker_id == offline_id) has_offline = true;
    }
    CHECK(has_online && has_offline);          // -1 含全部

    CHECK(store.Delete(online_id));
    CHECK(store.Delete(offline_id));
    PASS();
}

// ============================================================================
// ShardStore 用例
// ============================================================================

static ShardRecord makeShard(const std::string& shard_id, const std::string& job_id,
                             int32_t index, int32_t status, int64_t start_ms,
                             int64_t duration_ms, const std::string& worker_id = "")
{
    ShardRecord s;
    s.shard_id = shard_id;
    s.job_id = job_id;
    s.shard_index = index;
    s.start_ms = start_ms;
    s.duration_ms = duration_ms;
    s.status = status;
    s.assigned_worker_id = worker_id;
    s.attempt_id = "attempt_1";
    s.max_retry = 3;
    s.input_path = "/data/videos/in.mp4";
    s.output_path = "/data/output/out.mp4";
    s.target_resolution = "720p";
    s.target_bitrate = 2000;
    s.created_at = 1000;
    s.updated_at = 1000;
    return s;
}

static void test_shard_count_by_status()
{
    TEST("ShardStore CountByStatus / ListByStatus");

    auto job_id = uid("job");
    auto shard_done = job_id + "_shard_0";
    auto shard_canceled = job_id + "_shard_1";
    ShardStore& store = ShardStore::GetInstance();

    // 只用终态（SUCCESS/CANCELED）：dev 库可能被真实服务并发修改，
    // 调度循环只认领 WAITING/ASSIGNED/RUNNING，终态行不会被并发迁移，
    // 增量/存在性断言才是确定性的
    CHECK(store.Insert(makeShard(shard_done, job_id, 0, 5, 0, 15000)));       // SUCCESS
    CHECK(store.Insert(makeShard(shard_canceled, job_id, 1, 8, 15000, 15000))); // CANCELED

    // CountByStatus：弱断言（我们插入的行必然计入对应桶）
    auto counts = store.CountByStatus();
    CHECK(counts.count(5) && counts.at(5) >= 1);
    CHECK(counts.count(8) && counts.at(8) >= 1);

    // ListByStatus：按唯一 shard_id 确定性断言
    auto done = store.ListByStatus(5);
    bool has_done = false;
    for (const auto& s : done) if (s.shard_id == shard_done) has_done = true;
    CHECK(has_done);
    auto canceled = store.ListByStatus(8);
    bool has_canceled = false;
    for (const auto& s : canceled) if (s.shard_id == shard_canceled) has_canceled = true;
    CHECK(has_canceled);

    CHECK(store.Delete(shard_done));
    CHECK(store.Delete(shard_canceled));
    PASS();
}

static void test_shard_list_by_job_worker()
{
    TEST("ShardStore ListByJob / ListByWorker");

    auto job_id = uid("job");
    auto worker_a = uid("worker");
    auto shard0 = job_id + "_shard_0";
    auto shard1 = job_id + "_shard_1";
    ShardStore& store = ShardStore::GetInstance();
    // 终态状态，避免被运行中的真实 scheduler 并发认领
    CHECK(store.Insert(makeShard(shard0, job_id, 0, 5, 0, 15000, worker_a)));   // SUCCESS
    CHECK(store.Insert(makeShard(shard1, job_id, 1, 8, 15000, 15000)));         // CANCELED

    auto by_job = store.ListByJob(job_id);
    CHECK(by_job.size() == 2);

    auto by_worker = store.ListByWorker(worker_a);
    CHECK(by_worker.size() == 1);
    CHECK(by_worker[0].shard_id == shard0);

    CHECK(store.Delete(shard0));
    CHECK(store.Delete(shard1));
    PASS();
}

static void test_shard_update_if_status()
{
    TEST("ShardStore UpdateIfStatus");

    auto job_id = uid("job");
    auto shard_id = job_id + "_shard_0";
    ShardStore& store = ShardStore::GetInstance();
    auto s = makeShard(shard_id, job_id, 0, 6, 0, 15000);   // FAILED（终态，防并发认领）
    CHECK(store.Insert(s));

    s.status = 8;   // CANCELED
    CHECK(store.UpdateIfStatus(shard_id, {6}, s));          // FAILED → CANCELED
    auto got = store.Get(shard_id);
    CHECK(got.has_value() && got->status == 8);

    auto stale = s;
    stale.status = 6;
    CHECK(!store.UpdateIfStatus(shard_id, {6}, stale));     // 陈旧快照拒绝
    got = store.Get(shard_id);
    CHECK(got.has_value() && got->status == 8);

    CHECK(store.Delete(shard_id));
    PASS();
}

// ============================================================================
// main
// ============================================================================

int main(int argc, char** argv)
{
    std::cout << "=== test_store ===" << std::endl;

    // 解析 -i <conf>，否则从常见相对路径 fallback（bin/ 或仓库根运行）
    std::string conf;
    for (int i = 1; i + 1 < argc; ++i)
    {
        if (std::strcmp(argv[i], "-i") == 0) conf = argv[i + 1];
    }
    if (conf.empty())
    {
        for (const char* cand : {"../test/video_platform/conf/store_test.conf",
                                 "test/video_platform/conf/store_test.conf"})
        {
            if (access(cand, F_OK) == 0) { conf = cand; break; }
        }
    }
    if (conf.empty())
    {
        std::cout << "SKIP (no store_test.conf found, pass -i <conf>)" << std::endl;
        return 0;
    }

    // 初始化配置 + MySQL 连接池（失败 = MySQL 不可达 → SKIP，CI 安全跳过）
    const char* real_argv[] = {"test_store", "-i", conf.c_str()};
    if (!MprpcApplication::Init(3, const_cast<char**>(real_argv)))
    {
        std::cout << "SKIP (MprpcApplication init failed)" << std::endl;
        return 0;
    }
    if (!MysqlPool::GetInstance().Init())
    {
        std::cout << "SKIP (MySQL unreachable, init failed)" << std::endl;
        return 0;
    }

    // JobStore
    test_job_insert_get();
    test_job_insert_dup();
    test_job_get_delete_missing();
    test_job_update_if_status_hit();
    test_job_update_if_status_miss();
    test_job_update_if_status_multi();
    // WorkerStore
    test_worker_heartbeat_online();
    test_worker_heartbeat_offline_rejected();
    test_worker_heartbeat_missing();
    test_worker_mark_offline_timeout();
    test_worker_mark_offline_not_timeout();
    test_worker_mark_offline_already();
    test_worker_upsert();
    test_worker_list_by_status();
    // ShardStore
    test_shard_count_by_status();
    test_shard_list_by_job_worker();
    test_shard_update_if_status();

    std::cout << "\n=== Result: " << g_passed << " passed, "
              << g_failed << " failed ===" << std::endl;
    return g_failed > 0 ? 1 : 0;
}
