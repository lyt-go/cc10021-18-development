import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type ExchangeSnapshot = Record<string, unknown>

export type ImportResult = {
  快照编号: string
  文件指纹: string
  导入批次: string
  字段总数: number
  已适配字段: number
  未解决字段: number
  状态: string
  重复导入: boolean
  有效结构唯一: boolean
  说明: string
  表明细: unknown[]
}

export type ReconcileReport = {
  快照编号: string
  导入批次: string
  核对字段数: number
  一致字段数: number
  未解决项数: number
  全部一致: boolean
  核对明细: unknown[]
  待核清单回写: unknown
}

/** 导出一张表为带源类型的目录交换快照。 */
export async function exportExchangeSnapshot(
  tableName: string,
  sourceEnv = 'prod-mysql',
  targetEnv = 'dr-hive',
): Promise<ExchangeSnapshot> {
  const query = new URLSearchParams({
    table_name: tableName,
    source_env: sourceEnv,
    target_env: targetEnv,
  }).toString()
  const response = await request(`/api/table_catalog/exchange/snapshots/export?${query}`)
  if (!response.ok) throw new Error('交换快照导出失败')
  return await response.json()
}

/** 导入目录交换快照，同一文件重复导入只返回首次的有效结构。 */
export async function importExchangeSnapshot(
  snapshot: ExchangeSnapshot,
  targetEnv?: string,
  remark?: string,
): Promise<ImportResult> {
  const response = await request('/api/table_catalog/exchange/snapshots/import', {
    method: 'POST',
    body: JSON.stringify({ snapshot, target_env: targetEnv, remark }),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '交换快照导入失败')
  }
  return await response.json()
}

/** 对类型转换失败的字段重试适配，未解决列继续保留。 */
export async function retryExchangeSnapshot(
  snapshotId: number,
  typeOverrides: Record<string, string>,
): Promise<Record<string, unknown>> {
  const response = await request(
    `/api/table_catalog/exchange/snapshots/${snapshotId}/retry`,
    { method: 'POST', body: JSON.stringify({ type_overrides: typeOverrides }) },
  )
  if (!response.ok) throw new Error('未解决字段重试失败')
  return await response.json()
}

/** 按次核对上传文件源类型与已登记到目录的字段。 */
export async function reconcileExchangeSnapshot(
  snapshotId: number,
): Promise<ReconcileReport> {
  const response = await request(
    `/api/table_catalog/exchange/snapshots/${snapshotId}/reconcile`,
  )
  if (!response.ok) throw new Error('字段变更对比失败')
  return await response.json()
}
