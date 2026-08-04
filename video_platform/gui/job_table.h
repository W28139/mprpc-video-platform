#pragma once

// ============================================================================
// JobTableModel + JobTableView — 任务列表（阶段 12 GUI）
// ============================================================================
//
// 数据来自 ListJobs RPC（2s 轮询，概要不含 shard）。列：
//   状态 | Job ID | 输入文件 | 分辨率 | Shard | 优先级 | 创建时间
// 状态列颜色：SCHEDULING/RUNNING 蓝、SUCCESS 绿、FAILED 红、CANCELED 灰。
// 双击 SUCCESS 行 → 播放 merged.mp4（MainWindow 处理）。
// ============================================================================

#include <QAbstractTableModel>
#include <QTableView>

#include "video_platform/gui/rpc_client.h"

namespace video_platform {

/// @brief 任务列表数据模型
class JobTableModel : public QAbstractTableModel {
    Q_OBJECT
public:
    enum Column {
        ColStatus = 0,
        ColJobId,
        ColInput,
        ColResolution,
        ColShards,
        ColPriority,
        ColCreatedAt,
        ColCount
    };

    explicit JobTableModel(QObject* parent = nullptr);

    void setJobs(const std::vector<JobItem>& jobs);

    int rowCount(const QModelIndex& parent = QModelIndex()) const override;
    int columnCount(const QModelIndex& parent = QModelIndex()) const override;
    QVariant data(const QModelIndex& index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation,
                        int role) const override;

    const JobItem* jobAt(int row) const;

private:
    std::vector<JobItem> jobs_;
};

/// @brief 任务表格视图（双击信号透传）
class JobTableView : public QTableView {
    Q_OBJECT
public:
    explicit JobTableView(QWidget* parent = nullptr);
};

} // namespace video_platform
