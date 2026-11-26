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

export type DeliveryPayload = Record<string, unknown>

async function postJson(path: string, payload: DeliveryPayload): Promise<any> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return await response.json()
}

export async function listDeliveryHandoffs(): Promise<any[]> {
  const response = await request('/api/delivery/handoffs')
  if (!response.ok) throw new Error('部分签收交接读取失败')
  const payload = await response.json()
  return payload.items ?? []
}

export function createFourItemHandoff(payload: DeliveryPayload = {}): Promise<any> {
  return postJson('/api/delivery/scenarios/four-items', payload)
}

export function recordDeliveryReceipt(payload: DeliveryPayload): Promise<any> {
  return postJson('/api/delivery/receipts', payload)
}

export function previewRedelivery(payload: DeliveryPayload): Promise<any> {
  return postJson('/api/delivery/redelivery/preview', payload)
}

export function applyRedelivery(payload: DeliveryPayload): Promise<any> {
  return postJson('/api/delivery/redelivery/apply', payload)
}

export function claimRedelivery(payload: DeliveryPayload): Promise<any> {
  return postJson('/api/delivery/redelivery/claims', payload)
}

export async function getDeliveryTrace(params: {
  batchId?: string | number
  exportId?: string | number
}): Promise<any> {
  const query = new URLSearchParams()
  if (params.batchId !== undefined) query.set('batch_id', String(params.batchId))
  if (params.exportId !== undefined) query.set('export_id', String(params.exportId))
  const response = await request(`/api/delivery/trace?${query.toString()}`)
  if (!response.ok) throw new Error('补投与签收追溯读取失败')
  return await response.json()
}

export function deliveryManifestUrl(batchId?: string | number): string {
  const query = new URLSearchParams()
  if (batchId !== undefined) query.set('batch_id', String(batchId))
  return `/api/export_dataset/delivery-manifest.csv?${query.toString()}`
}
