<template>
  <section class="export-trace-panel" data-testid="export-trace-panel">
    <div class="trace-head">
      <div>
        <h3>补投申请与签收结果追溯</h3>
        <p>按同一批次回查这次补投及原接收记录；签收结果回写到导出清单，并明确区分等待核对项与明确失败项。</p>
      </div>
      <div class="trace-controls">
        <input
          v-model="batchId"
          class="trace-input"
          type="text"
          placeholder="输入批次号，如 BATCH-PARTIAL-4"
          data-testid="trace-batch-input"
          @keyup.enter="loadTrace()"
        />
        <button class="btn primary" type="button" data-testid="query-trace" @click="loadTrace()">查询追溯</button>
        <button class="btn" type="button" data-testid="refresh-trace" @click="loadTrace()">刷新</button>
        <a
          v-if="batchId"
          class="btn"
          :href="manifestUrl"
          data-testid="download-manifest"
        >下载条目签收清单</a>
      </div>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="trace-error">{{ errorMessage }}</p>
    <div v-if="!trace && !errorMessage" class="trace-empty">正在读取同批次追溯信息…</div>

    <template v-if="trace">
      <div class="trace-summary" data-testid="trace-summary" :data-batch-id="trace.batch_id">
        <span>批次：<strong data-testid="trace-batch-id">{{ trace.batch_id || batchId }}</strong></span>
        <span class="signed">已签收 {{ trace.signed_count ?? 0 }}</span>
        <span class="failed">明确失败 {{ trace.failed_count ?? 0 }}</span>
        <span class="waiting">等待核对 {{ trace.waiting_verification_count ?? trace.unknown_count ?? 0 }}</span>
        <span>可补投 {{ trace.eligible_item_ids?.length ?? 0 }}</span>
        <span v-if="trace.legacy" class="legacy">历史整批回执兼容模式</span>
      </div>

      <div class="trace-columns">
        <div class="trace-card failed-card">
          <h4>明确失败项（不与等待核对混合）</h4>
          <ul data-testid="explicit-failed-items">
            <li v-for="itemId in trace.explicit_failed_item_ids || []" :key="`failed-${itemId}`"
                :data-testid="`explicit-failed-${itemId}`">{{ itemId }}</li>
            <li v-if="!(trace.explicit_failed_item_ids || []).length" class="muted">无</li>
          </ul>
        </div>
        <div class="trace-card waiting-card">
          <h4>已提交但结果未知 / 等待核对</h4>
          <ul data-testid="waiting-items">
            <li v-for="itemId in trace.waiting_verification_item_ids || []" :key="`waiting-${itemId}`"
                :data-testid="`waiting-item-${itemId}`">{{ itemId }}</li>
            <li v-if="!(trace.waiting_verification_item_ids || []).length" class="muted">无</li>
          </ul>
        </div>
        <div class="trace-card scope-card">
          <h4>最新可补投范围</h4>
          <p data-testid="trace-eligible-scope">{{ formatIds(trace.eligible_item_ids) }}</p>
        </div>
      </div>

      <h4>导出清单条目回写</h4>
      <table class="data-table manifest-table">
        <thead>
          <tr>
            <th>条目</th>
            <th>签收结果</th>
            <th>回执编号</th>
            <th>补投申请</th>
            <th>领取记录</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in trace.items || []"
            :key="String(item.item_id)"
            :data-testid="`trace-item-${item.item_id}`"
            :data-status="item.status"
          >
            <td>{{ item.item_id }}</td>
            <td>{{ item.签收状态 || item.status }}</td>
            <td>{{ (item.receipt_ids || []).join(', ') || '—' }}</td>
            <td>{{ (item.redelivery_ids || []).join(', ') || '—' }}</td>
            <td>{{ (item.claim_ids || []).join(', ') || '—' }}</td>
          </tr>
          <tr v-if="!(trace.items || []).length">
            <td colspan="5" class="empty-state">历史批次仅保存整批回执，未提供可追踪的逐条范围</td>
          </tr>
        </tbody>
      </table>

      <h4>本次补投申请</h4>
      <div data-testid="redelivery-history">
        <article
          v-for="application in trace.redelivery_requests || []"
          :key="application.request_no"
          class="application-record"
          :data-testid="`trace-application-${application.request_no}`"
        >
          <strong>{{ application.request_no }}</strong>
          <span>正式范围：<b :data-testid="`trace-application-scope-${application.request_no}`">{{ formatIds(application.accepted_item_ids) }}</b></span>
          <span v-if="application.removed_item_ids?.length">移除：{{ formatIds(application.removed_item_ids) }}</span>
          <span>领取：{{ application.claims?.join(', ') || '尚未领取' }}</span>
        </article>
        <p v-if="!(trace.redelivery_requests || []).length" class="muted">同一批次暂无补投申请。</p>
      </div>

      <h4>原接收记录</h4>
      <table class="data-table receipt-table">
        <thead>
          <tr><th>回执号</th><th>条目</th><th>结果</th><th>时间</th></tr>
        </thead>
        <tbody data-testid="original-receipts">
          <tr v-for="receipt in trace.original_receipts || []" :key="receipt.receipt_id">
            <td>{{ receipt.receipt_id }}</td>
            <td>{{ receipt.item_id }}</td>
            <td>{{ receipt.签收结果 || receipt.status }}</td>
            <td>{{ receipt.received_at }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { deliveryManifestUrl, getDeliveryTrace, listDeliveryHandoffs } from '@/api/backend-service'

const props = defineProps<{ exportId?: number | string }>()

const batchId = ref('')
const trace = ref<any>(null)
const errorMessage = ref('')

const manifestUrl = computed(() => deliveryManifestUrl(batchId.value))

function formatIds(ids: unknown[] | undefined | null): string {
  if (!ids || !ids.length) return '空集合'
  return ids.map((id) => String(id)).join('、')
}

async function loadTrace(exportId?: number | string) {
  errorMessage.value = ''
  try {
    trace.value = await getDeliveryTrace({
      batchId: batchId.value || undefined,
      exportId: exportId ?? props.exportId,
    })
    if (trace.value?.ok === false) throw new Error(trace.value.message || '追溯信息不存在')
    if (!batchId.value && trace.value?.batch_id) batchId.value = trace.value.batch_id
  } catch (error) {
    trace.value = null
    errorMessage.value = error instanceof Error ? error.message : '追溯查询失败'
  }
}

async function openForRow(row: Record<string, any>) {
  const nextBatch = row._delivery_batch_id || row.数据批次 || row.批次号
  if (nextBatch) batchId.value = String(nextBatch)
  await loadTrace(row.id)
}

defineExpose({ openForRow })

onMounted(async () => {
  try {
    const handoffs = await listDeliveryHandoffs()
    if (handoffs[0]?.batch_id) batchId.value = handoffs[0].batch_id
  } catch {
    // 追溯接口会给出更具体的错误；这里保留手工输入批次号。
  }
  await loadTrace()
})
</script>

<style scoped>
.export-trace-panel {
  margin: 18px 0;
  padding: 18px;
  border: 1px solid #d9e2ec;
  border-radius: 14px;
  background: #f8fafc;
}
.trace-head h3 { margin: 0 0 4px; }
.trace-head p { color: #64748b; font-size: 13px; }
.trace-controls { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 10px; }
.trace-input { min-width: 280px; padding: 8px 10px; border: 1px solid #cbd5e1; border-radius: 8px; }
.trace-summary { display: flex; gap: 14px; flex-wrap: wrap; margin: 16px 0; padding: 12px; background: white; border-radius: 10px; }
.trace-summary .signed { color: #15803d; }
.trace-summary .failed { color: #b91c1c; }
.trace-summary .waiting { color: #a16207; }
.legacy { color: #64748b; }
.trace-columns { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 16px; }
.trace-card { background: white; border-radius: 10px; padding: 12px; border: 1px solid #e2e8f0; }
.trace-card h4 { margin-top: 0; }
.failed-card { border-top: 3px solid #dc2626; }
.waiting-card { border-top: 3px solid #ca8a04; }
.scope-card { border-top: 3px solid #2563eb; }
.muted { color: #64748b; font-size: 13px; }
.application-record { display: flex; gap: 14px; flex-wrap: wrap; background: white; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; }
.error-text { color: #b91c1c; }
.trace-empty { color: #64748b; padding: 12px 0; }
</style>
