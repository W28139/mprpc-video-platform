#pragma once

// ============================================================================
// WorkerTableModel + WorkerPanel — Worker 负载面板（阶段 12 GUI）
// ============================================================================
//
// 数据来自 ListWorkers RPC（2s 轮询）。列：
//   Worker ID | IP:Port | 状态 | CPU% | 内存% | 运行/上限
// 负载高亮：CPU 或内存 >= 80% 红、>= 50% 黄（阶段 7 过载保护阈值同源）。
// 槽位显示 `running/max`，满载时黄色提醒。
// ============================================================================

#include <QAbstractTableModel>
#include <QTableView>
#include <QWidget>

#include "video_platform/gui/rpc_client.h"

namespace video_platform {

/// @brief Worker 负载数据模型
class WorkerTableModel : public QAbstractTableModel {
    Q_OBJECT
public:
    enum Column {
        ColWorkerId = 0,
        ColAddress,
        ColStatus,
        ColCpu,
        ColMem,
        ColSlots,
        ColCount
    };

    explicit WorkerTableModel(QObject* parent = nullptr);

    void setWorkers(const std::vector<WorkerItem>& workers);

    int rowCount(const QModelIndex& parent = QModelIndex()) const override;
    int columnCount(const QModelIndex& parent = QModelIndex()) const override;
    QVariant data(const QModelIndex& index, int role) const override;
    QVariant headerData(int section, Qt::Orientation orientation,
                        int role) const override;

private:
    std::vector<WorkerItem> workers_;
};

/// @brief Worker 面板（表格 + 标题行）
class WorkerPanel : public QWidget {
    Q_OBJECT
public:
    explicit WorkerPanel(QWidget* parent = nullptr);

    /// @brief 刷新数据（UI 线程调用，来自 RPC 信号）
    void updateWorkers(const std::vector<WorkerItem>& workers);

private:
    WorkerTableModel* model_;
};

} // namespace video_platform
