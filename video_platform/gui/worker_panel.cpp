#include "video_platform/gui/worker_panel.h"

#include <QHBoxLayout>
#include <QHeaderView>
#include <QLabel>

namespace video_platform {

static QString workerStatusText(int status)
{
    switch (status) {
        case 1: return "ONLINE";
        case 2: return "OFFLINE";
        case 3: return "DRAINING";
        default: return "UNKNOWN";
    }
}

// 负载分级：>=80 红（危险）、>=50 黄（偏高）、其余正常（深色主题亮色调）
static QColor loadColor(int usage)
{
    if (usage >= 80) return QColor(0xf8, 0x71, 0x71);
    if (usage >= 50) return QColor(0xf5, 0xc2, 0x11);
    return QColor(0x34, 0xd3, 0x99);
}

WorkerTableModel::WorkerTableModel(QObject* parent)
    : QAbstractTableModel(parent) {}

void WorkerTableModel::setWorkers(const std::vector<WorkerItem>& workers)
{
    beginResetModel();
    workers_ = workers;
    endResetModel();
}

int WorkerTableModel::rowCount(const QModelIndex& parent) const
{
    return parent.isValid() ? 0 : static_cast<int>(workers_.size());
}

int WorkerTableModel::columnCount(const QModelIndex& parent) const
{
    return parent.isValid() ? 0 : ColCount;
}

QVariant WorkerTableModel::data(const QModelIndex& index, int role) const
{
    if (!index.isValid() || index.row() < 0 ||
        index.row() >= static_cast<int>(workers_.size()))
        return {};

    const WorkerItem& w = workers_[index.row()];

    if (role == Qt::DisplayRole)
    {
        switch (index.column()) {
            case ColWorkerId: return w.worker_id;
            case ColAddress:
                return QString("%1:%2").arg(w.ip).arg(w.port);
            case ColStatus:   return workerStatusText(w.status);
            case ColCpu:      return QString("%1%").arg(w.cpu_usage);
            case ColMem:      return QString("%1%").arg(w.memory_usage);
            case ColSlots:
                return QString("%1/%2").arg(w.running_shards)
                                        .arg(w.max_running_shards);
            default:          return {};
        }
    }

    // 状态列：OFFLINE 灰色
    if (role == Qt::ForegroundRole && index.column() == ColStatus)
    {
        if (w.status == 2) return QColor(0x9c, 0xa3, 0xaf);
        return QColor(0x34, 0xd3, 0x99);
    }

    // 负载列：颜色分级
    if (role == Qt::ForegroundRole &&
        (index.column() == ColCpu || index.column() == ColMem))
    {
        int usage = (index.column() == ColCpu) ? w.cpu_usage : w.memory_usage;
        return loadColor(usage);
    }

    // 槽位：满载黄色提醒
    if (role == Qt::ForegroundRole && index.column() == ColSlots &&
        w.max_running_shards > 0 &&
        w.running_shards >= w.max_running_shards)
        return QColor(0xf5, 0xc2, 0x11);

    if (role == Qt::TextAlignmentRole &&
        (index.column() == ColCpu || index.column() == ColMem ||
         index.column() == ColSlots))
        return Qt::AlignCenter;

    return {};
}

QVariant WorkerTableModel::headerData(int section, Qt::Orientation orientation,
                                      int role) const
{
    if (role != Qt::DisplayRole || orientation != Qt::Horizontal) return {};
    switch (section) {
        case ColWorkerId: return "Worker";
        case ColAddress:  return "地址";
        case ColStatus:   return "状态";
        case ColCpu:      return "CPU";
        case ColMem:      return "内存";
        case ColSlots:    return "运行/上限";
        default:          return {};
    }
}

// ── 面板 ──────────────────────────────────────────────────────────────────

WorkerPanel::WorkerPanel(QWidget* parent) : QWidget(parent)
{
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);

    auto* header = new QLabel("Worker 负载（每 2s 刷新；红 = 负载 ≥80%，黄 = ≥50%）",
                              this);
    header->setProperty("role", "hint");
    header->setContentsMargins(4, 2, 4, 2);
    layout->addWidget(header);

    model_ = new WorkerTableModel(this);
    auto* table = new QTableView(this);
    table->setModel(model_);
    table->setSelectionBehavior(QAbstractItemView::SelectRows);
    table->setEditTriggers(QAbstractItemView::NoEditTriggers);
    table->setAlternatingRowColors(true);
    table->verticalHeader()->setVisible(false);
    table->verticalHeader()->setDefaultSectionSize(36);
    table->horizontalHeader()->setStretchLastSection(true);
    table->horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);
    table->setColumnWidth(WorkerTableModel::ColWorkerId, 150);
    layout->addWidget(table, 1);
}

void WorkerPanel::updateWorkers(const std::vector<WorkerItem>& workers)
{
    model_->setWorkers(workers);
}

} // namespace video_platform
