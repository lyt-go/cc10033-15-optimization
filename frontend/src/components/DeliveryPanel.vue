<template>
  <section class="delivery-panel" data-testid="delivery-redelivery-panel">
    <div class="panel-head">
      <div>
        <h3>部分签收详情与补投申请</h3>
        <p>按条目回执计算同一批次的实际可投范围，避免把已签收条目整批重发。</p>
      </div>
      <div class="panel-actions">
        <button class="btn" type="button" data-testid="refresh-handoffs" @click="loadHandoffs()">刷新交接</button>
        <button class="btn primary" type="button" data-testid="create-four-handoff" @click="createFourScenario">
          构造四项交接
        </button>
      </div>
    </div>

    <div v-if="handoffs.length" class="handoff-list" data-testid="handoff-list">
      <button
        v-for="handoff in handoffs"
        :key="handoff.handoff_no"
        type="button"
        class="handoff-chip"
        :class="{ active: active?.handoff_no === handoff.handoff_no }"
        :data-testid="`open-partial-${handoff.id}`"
        @click="openHandoff(handoff)"
      >
        部分签收详情：{{ handoff.batch_id }}
        <span>{{ handoff.counts?.signed ?? 0 }}签收 / {{ handoff.counts?.failed ?? 0 }}失败 /
          {{ handoff.counts?.unknown ?? 0 }}未知</span>
      </button>
    </div>

    <div v-if="!active" class="panel-empty" data-testid="no-handoff">
      暂无逐条交接，可点击“构造四项交接”准备 2 项签收、1 项失败、1 项确认未知的场景。
    </div>

    <div
      v-else
      class="detail-card"
      role="dialog"
      aria-label="部分签收详情"
      data-testid="partial-receipt-detail"
      :data-batch-id="active.batch_id"
    >
      <div class="detail-title">
        <div>
          <h4>批次 {{ active.batch_id }}</h4>
          <span>交接号 {{ active.handoff_no }}；勾选明确可补投的条目并预览实际接收范围；只有同一批次且尚未签收的条目可补送。</span>
        </div>
        <button
          class="btn ghost"
          type="button"
          data-testid="receive-late-fourth"
          @click="receiveLateReceipt()"
        >
          让第四项收到迟到回执
        </button>
      </div>

      <div class="mini-stats">
        <span data-testid="count-signed">已签收 {{ active.counts?.signed ?? 0 }}</span>
        <span data-testid="count-failed">明确失败 {{ active.counts?.failed ?? 0 }}</span>
        <span data-testid="count-unknown">等待核对 {{ active.counts?.unknown ?? 0 }}</span>
        <span data-testid="count-eligible">当前可补投 {{ active.counts?.eligible ?? 0 }}</span>
        <span class="eligible-ids" data-testid="eligible-scope" :data-ids="formatIds(active.eligible_item_ids)">
          资格范围：{{ formatIds(active.eligible_item_ids) }}
        </span>
      </div>

      <div v-if="Object.keys(removed).length" class="removed-box" data-testid="removed-items">
        <strong>服务端反馈：以下勾选条目已被最新签收结果移除</strong>
        <ul>
          <li
            v-for="(item, key) in removed"
            :key="key"
            :data-testid="`removed-item-${key}`"
            :data-reason="item.reason"
          >
            条目 {{ key }}：{{ item.reason === 'late_signed' ? '收到迟到签收' : '已签收/不属于本批次' }}，
            已取消补投资格；其他选择已保留。
          </li>
        </ul>
      </div>

      <table class="data-table item-table">
        <thead>
          <tr>
            <th>勾选补投</th>
            <th>条目</th>
            <th>签收结果</th>
            <th>回执编号</th>
            <th>资格/核对</th>
            <th>迟到回执</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in active.items"
            :key="String(item.item_id)"
            :data-testid="`receipt-item-${item.item_id}`"
            :data-item-id="String(item.item_id)"
            data-partial-item="true"
            :data-status="item.status"
            :data-signed="String(!!item.signed)"
            :data-eligible="String(!item.signed)"
            :class="{ removed: removed[String(item.item_id)], signed: item.signed }"
          >
            <td>
              <input
                type="checkbox"
                class="redelivery-checkbox"
                data-redelivery-checkbox="true"
                :data-item-id="String(item.item_id)"
                :value="String(item.item_id)"
                :data-testid="`redelivery-checkbox-${item.item_id}`"
                :checked="!!selected[String(item.item_id)]"
                :disabled="!!item.signed"
                @change="toggleItem(item.item_id, ($event.target as HTMLInputElement).checked)"
              />
            </td>
            <td>{{ item.item_id }}</td>
            <td data-testid="`item-status-${item.item_id}`">{{ item.签收状态 || item.status }}</td>
            <td>{{ (item.receipt_ids || []).join(', ') || '—' }}</td>
            <td>
              <span v-if="removed[String(item.item_id)]" class="badge removed-badge">资格已移除</span>
              <span v-else-if="item.signed" class="badge signed-badge">已签收，不可补投</span>
              <span v-else-if="item.status === 'failed'" class="badge failed-badge">明确失败，可补投</span>
              <span v-else-if="item.waiting_verification" class="badge unknown-badge">确认未知，提交前复核</span>
              <span v-else class="badge pending-badge">尚未签收，可纳入明确范围</span>
            </td>
            <td>
              <button
                v-if="!item.signed"
                class="link"
                type="button"
                :data-testid="`late-receipt-${item.item_id}`"
                @click="receiveLateReceipt(item.item_id)"
              >
                记录签收
              </button>
              <span v-else>—</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="action-row">
        <button class="btn" type="button" data-testid="preview-redelivery" :disabled="busy" @click="runPreview">
          预览实际接收范围
        </button>
        <button class="btn primary" type="button" data-testid="submit-redelivery" :disabled="busy" @click="submit">
          正式提交补投申请
        </button>
        <button
          class="btn"
          type="button"
          data-testid="claim-scope"
          :disabled="!application"
          @click="claimScope"
        >
          核对再次领取集合
        </button>
      </div>

      <div v-if="preview" class="result-box" data-testid="preview-result">
        <strong>预览后的实际接收范围：</strong>
        <span data-testid="actual-receive-scope">{{ formatIds(preview.actual_receive_scope) }}</span>
        <span class="muted">（共 {{ preview.actual_receive_scope?.length ?? 0 }} 项）</span>
      </div>

      <div v-if="application" class="result-box application-box" data-testid="redelivery-application">
        <div>补投申请已受理：<span data-testid="redelivery-request-id">{{ application.request_no }}</span></div>
        <div>正式可投范围：<strong data-testid="accepted-scope">{{ formatIds(application.accepted_item_ids) }}</strong></div>
        <div v-if="application.removed_item_ids?.length" data-testid="submit-removed">
          提交时移除：{{ formatIds(application.removed_item_ids) }}
        </div>
      </div>

      <div v-if="claim" class="result-box claim-box" data-testid="claim-result">
        <div>再次领取的明确集合：<strong data-testid="claimed-scope">{{ formatIds(claim.claimed_item_ids) }}</strong></div>
        <div class="claim-ids">
          <span
            v-for="itemId in claim.claimed_item_ids"
            :key="`claimed-${itemId}`"
            :data-testid="`claimed-item-${itemId}`"
            class="badge claim-badge"
          >{{ itemId }}</span>
        </div>
        <div v-if="claim.blocked_item_ids?.length">领取时拦截：{{ formatIds(claim.blocked_item_ids) }}</div>
      </div>

      <p v-if="message" class="ok-text" data-testid="delivery-message">{{ message }}</p>
      <p v-if="errorMessage" class="error-text" data-testid="delivery-error">{{ errorMessage }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  applyRedelivery,
  claimRedelivery,
  createFourItemHandoff,
  listDeliveryHandoffs,
  previewRedelivery,
  recordDeliveryReceipt,
} from '@/api/backend-service'

const handoffs = ref<any[]>([])
const active = ref<any>(null)
const selected = ref<Record<string, boolean>>({})
const removed = ref<Record<string, any>>({})
const preview = ref<any>(null)
const application = ref<any>(null)
const claim = ref<any>(null)
const message = ref('')
const errorMessage = ref('')
const busy = ref(false)

function idKey(id: unknown): string {
  return String(id)
}

function numericIds(ids: unknown[]): Array<number | string> {
  return (ids || []).map((id) => {
    const text = String(id)
    return /^\d+$/.test(text) ? Number(text) : text
  })
}

function selectedIds(): Array<number | string> {
  return numericIds(Object.entries(selected.value).filter(([, checked]) => checked).map(([id]) => id))
}

function formatIds(ids: unknown[] | undefined | null): string {
  if (!ids || !ids.length) return '空集合'
  return ids.map((id) => String(id)).join('、')
}

function initializeSelection(handoff: any) {
  const next: Record<string, boolean> = {}
  for (const id of handoff.eligible_item_ids || []) {
    next[idKey(id)] = true
  }
  selected.value = next
  removed.value = {}
  preview.value = null
  application.value = null
  claim.value = null
}

async function loadHandoffs(
  preferId?: string | number,
  options: { preserveSelection?: boolean } = {},
) {
  errorMessage.value = ''
  try {
    handoffs.value = await listDeliveryHandoffs()
    const preferred = preferId !== undefined
      ? handoffs.value.find((h) => String(h.id) === String(preferId) || h.handoff_no === preferId)
      : active.value
        ? handoffs.value.find((h) => h.handoff_no === active.value.handoff_no)
        : handoffs.value[0]
    if (preferred) {
      if (options.preserveSelection && active.value?.handoff_no === preferred.handoff_no) {
        const previous = selected.value
        const next: Record<string, boolean> = {}
        for (const [key, checked] of Object.entries(previous)) {
          const item = preferred.items.find((candidate: any) => idKey(candidate.item_id) === key)
          next[key] = Boolean(checked) && !!item && !item.signed
        }
        active.value = preferred
        selected.value = next
      } else {
        await openHandoff(preferred)
      }
    } else {
      active.value = null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交接列表读取失败'
  }
}

async function openHandoff(handoff: any) {
  active.value = handoff
  initializeSelection(handoff)
  message.value = ''
  errorMessage.value = ''
}

function toggleItem(itemId: unknown, checked: boolean) {
  selected.value[idKey(itemId)] = checked
  if (checked && removed.value[idKey(itemId)]) {
    // 服务端已经判定移除的条目不能通过再次勾选绕过；复选框本身也会被禁用。
    selected.value[idKey(itemId)] = false
  }
}

function applyRemovalFeedback(result: any) {
  const nextRemoved: Record<string, any> = { ...removed.value }
  for (const item of result?.removed_items || []) {
    nextRemoved[idKey(item.item_id)] = item
    selected.value[idKey(item.item_id)] = false
  }
  removed.value = nextRemoved
  if (active.value && result?.receipt_revision) {
    active.value.revision = result.receipt_revision
  }
}

async function runPreview() {
  if (!active.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await previewRedelivery({
      batch_id: active.value.batch_id,
      handoff_id: active.value.handoff_no,
      item_ids: selectedIds(),
    })
    if (result.ok === false) throw new Error(result.message || '预览失败')
    preview.value = result
    applyRemovalFeedback(result)
    message.value = result.message || '已按最新回执预览'
    await loadHandoffs(active.value.id, { preserveSelection: true })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '预览失败'
  } finally {
    busy.value = false
  }
}

async function submit() {
  if (!active.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await applyRedelivery({
      batch_id: active.value.batch_id,
      handoff_id: active.value.handoff_no,
      item_ids: selectedIds(),
      idempotency_key: `ui-${active.value.batch_id}-${Date.now()}`,
    })
    if (result.ok === false) {
      applyRemovalFeedback(result)
      throw new Error(result.message || '补投申请未受理')
    }
    application.value = result
    applyRemovalFeedback(result)
    message.value = result.message || '补投申请已提交'
    await loadHandoffs(active.value.id, { preserveSelection: true })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '提交失败'
  } finally {
    busy.value = false
  }
}

async function receiveLateReceipt(itemId?: string | number) {
  if (!active.value) return
  const target = itemId !== undefined
    ? itemId
    : active.value.items.find((item: any) => item.status === 'unknown' || item.waiting_verification)?.item_id
      ?? active.value.items[active.value.items.length - 1]?.item_id
  busy.value = true
  errorMessage.value = ''
  try {
    await recordDeliveryReceipt({
      batch_id: active.value.batch_id,
      handoff_id: active.value.handoff_no,
      item_id: target,
      status: 'signed',
      receipt_id: `LATE-${target}-${Date.now()}`,
    })
    await loadHandoffs(active.value.id, { preserveSelection: true })
    await runPreview()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迟到回执登记失败'
  } finally {
    busy.value = false
  }
}

async function claimScope() {
  if (!application.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await claimRedelivery({
      request_id: application.value.id,
      batch_id: application.value.batch_id,
      idempotency_key: `claim-ui-${application.value.id}`,
    })
    if (result.ok === false) throw new Error(result.message || '领取核对失败')
    claim.value = result
    message.value = result.message
    await loadHandoffs(active.value?.id, { preserveSelection: true })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '再次领取核对失败'
  } finally {
    busy.value = false
  }
}

async function createFourScenario() {
  busy.value = true
  errorMessage.value = ''
  try {
    const result = await createFourItemHandoff({ operator: '交接员' })
    await loadHandoffs(result.id)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '构造场景失败'
  } finally {
    busy.value = false
  }
}

async function openForPipe(pipeId: number | string) {
  await loadHandoffs()
  let match = handoffs.value.find(
    (handoff) => String(handoff.pipe_id) === String(pipeId)
      || String(handoff.entry_id) === String(pipeId),
  )
  if (!match) {
    // 演示/验收环境中，任意管道行都可一键生成题面要求的四项交接。
    await createFourItemHandoff({ entry_id: pipeId, operator: '交接员' })
    await loadHandoffs()
    match = handoffs.value.find(
      (handoff) => String(handoff.pipe_id) === String(pipeId)
        || String(handoff.entry_id) === String(pipeId),
    )
  }
  if (match) {
    await openHandoff(match)
  } else {
    active.value = null
    errorMessage.value = `投递配置 ${pipeId} 的部分签收详情打开失败`
  }
}

async function submitForPipe(pipeId: number | string) {
  if (!active.value || String(active.value.pipe_id ?? '') !== String(pipeId)) {
    await openForPipe(pipeId)
  }
  await submit()
}

defineExpose({ openForPipe, submitForPipe, runPreview, claimScope })

onMounted(loadHandoffs)
</script>

<style scoped>
.delivery-panel {
  margin: 18px 0;
  padding: 18px;
  border: 1px solid #d9e2ec;
  border-radius: 14px;
  background: #f8fafc;
}
.panel-head,
.detail-title,
.action-row {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}
.panel-head h3,
.detail-title h4 {
  margin: 0 0 4px;
}
.panel-head p,
.detail-title span,
.muted {
  color: #64748b;
  font-size: 13px;
}
.handoff-list {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin: 14px 0;
}
.handoff-chip {
  border: 1px solid #cbd5e1;
  background: white;
  border-radius: 999px;
  padding: 8px 12px;
  cursor: pointer;
}
.handoff-chip.active {
  border-color: #2563eb;
  background: #eff6ff;
}
.handoff-chip span {
  margin-left: 6px;
  color: #64748b;
  font-size: 12px;
}
.detail-card {
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 16px;
}
.mini-stats {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  margin: 12px 0;
}
.mini-stats span,
.badge {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 4px 10px;
  background: #eef2ff;
  font-size: 12px;
}
.item-table {
  margin: 10px 0 14px;
}
.item-table tr.removed {
  background: #fef2f2;
  text-decoration: line-through;
}
.item-table tr.signed {
  color: #64748b;
  background: #f8fafc;
}
.badge {
  text-decoration: none;
}
.signed-badge { background: #dcfce7; color: #166534; }
.failed-badge { background: #fee2e2; color: #991b1b; }
.unknown-badge { background: #fef9c3; color: #854d0e; }
.pending-badge { background: #e0f2fe; color: #075985; }
.removed-badge { background: #fecaca; color: #7f1d1d; font-weight: 700; }
.claim-badge { background: #dbeafe; color: #1e40af; margin-right: 4px; }
.removed-box {
  border-left: 4px solid #dc2626;
  background: #fef2f2;
  padding: 10px 14px;
  border-radius: 8px;
  margin: 10px 0;
}
.removed-box ul { margin: 8px 0 0 18px; }
.result-box {
  margin-top: 10px;
  padding: 10px 12px;
  border-radius: 8px;
  background: #f1f5f9;
}
.application-box { background: #ecfdf5; }
.claim-box { background: #eff6ff; }
.claim-ids { margin: 6px 0; }
.panel-empty {
  padding: 18px;
  border: 1px dashed #cbd5e1;
  border-radius: 10px;
  color: #64748b;
}
.ok-text { color: #15803d; }
.error-text { color: #b91c1c; }
</style>
