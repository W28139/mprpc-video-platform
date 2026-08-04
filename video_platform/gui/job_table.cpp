#include "video_platform/gui/job_table.h"

#include <QDateTime>
#include <QHeaderView>

namespace video_platform {

// 任务状态 → 中文显示（与 job_client.cpp 的状态码对应）
static QString jobStatusText(int status)
{
    switch (status) {
        case 0: return "UNKNOWN";
        case 1: return "PENDING";
        case 2: return "SPLITTING";
        case 3: return "SCHEDULING";
        case 4: return "RUNNING";
        case 5: return "MERGING";
        case 6: return "SUCCESS";
        case 7: return "FAILED";
        case 8: return "CANCELED";
        default: return "?";
    }
}

// 状态 → 前景色（深色主题下用亮色调保证可读性）
static QColor jobStatusColor(int status)
{
    switch (status) {
        case 3: case 4: case 5: return QColor(0x4d, 0x9f, 0xff);  // 进行中：亮蓝
        case 6: return QColor(0x34, 0xd3, 0x99);                  // 成功：亮绿
        case 7: return QColor(0xf8, 0x71, 0x71);                  // 失败：亮红
        case 8: return QColor(0x9c, 0xa3, 0xaf);                  // 取消：灰
        default: return QColor(0x9a, 0xa0, 0xb5);
    }
}

JobTableModel::JobTableModel(QObject* parent) : QAbstractTableModel(parent) {}

void JobTableModel::setJobs(const std::vector<JobItem>& jobs)
{
    beginResetModel();
    jobs_ = jobs;
    endResetModel();
}

int JobTableModel::rowCount(const QModelIndex& parent) const
{
    return parent.isValid() ? 0 : static_cast<int>(jobs_.size());
}

int JobTableModel::columnCount(const QModelIndex& parent) const
{
    return parent.isValid() ? 0 : ColCount;
}

QVariant JobTableModel::data(const QModelIndex& index, int role) const
{
    if (!index.isValid() || index.row() < 0 ||
        index.row() >= static_cast<int>(jobs_.size()))
        return {};

    const JobItem& j = jobs_[index.row()];

    if (role == Qt::DisplayRole)
    {
        switch (index.column()) {
            case ColStatus:
                return jobStatusText(j.status);
            case ColJobId:
                return j.job_id;
            case ColInput:
                return j.input_path;
            case ColResolution:
                return j.target_resolution.isEmpty() ? "原分辨率"
                                                     : j.target_resolution;
            case ColShards:
                return j.shard_count;
            case ColPriority:
                return j.priority;
            case ColCreatedAt:
                return QDateTime::fromSecsSinceEpoch(j.created_at / 1000)
                    .toString("MM-dd HH:mm:ss");
            default:
                return {};
        }
    }

    if (role == Qt::ForegroundRole && index.column() == ColStatus)
        return jobStatusColor(j.status);

    if (role == Qt::TextAlignmentRole && index.column() == ColShards)
        return Qt::AlignCenter;

    return {};
}

QVariant JobTableModel::headerData(int section, Qt::Orientation orientation,
                                   int role) const
{
    if (role != Qt::DisplayRole) return {};
    if (orientation != Qt::Horizontal) return {};
    switch (section) {
        case ColStatus:    return "状态";
        case ColJobId:     return "Job ID";
        case ColInput:     return "输入文件";
        case ColResolution: return "分辨率";
        case ColShards:    return "Shard";
        case ColPriority:  return "优先级";
        case ColCreatedAt: return "创建时间";
        default:           return {};
    }
}

const JobItem* JobTableModel::jobAt(int row) const
{
    if (row < 0 || row >= static_cast<int>(jobs_.size())) return nullptr;
    return &jobs_[row];
}

// ── 视图 ──────────────────────────────────────────────────────────────────

JobTableView::JobTableView(QWidget* parent) : QTableView(parent)
{
    setModel(new JobTableModel(this));
    setSelectionBehavior(QAbstractItemView::SelectRows);
    setSelectionMode(QAbstractItemView::SingleSelection);
    setEditTriggers(QAbstractItemView::NoEditTriggers);
    setAlternatingRowColors(true);
    setWordWrap(false);
    verticalHeader()->setVisible(false);
    verticalHeader()->setDefaultSectionSize(36);   // 行高：大字 + 可读性
    horizontalHeader()->setStretchLastSection(true);
    horizontalHeader()->setSectionResizeMode(QHeaderView::ResizeToContents);
    setColumnWidth(JobTableModel::ColJobId, 240);
    setColumnWidth(JobTableModel::ColInput, 340);
}

} // namespace video_platform
