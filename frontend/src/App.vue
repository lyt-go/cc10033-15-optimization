<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">分布式采集调度与反封锁平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向分布式采集任务调度、代理池管理、反封锁策略与结果质量校验的数据采集平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView v-slot="{ Component, route }">
        <component :is="Component" :key="route.path" />
      </RouterView>
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "采集任务", path: "/crawl_mission" }, { label: "代理节点", path: "/proxy_node" }, { label: "频率控制", path: "/rate_limit" }, { label: "设备指纹", path: "/fingerprint" }, { label: "验证码处理", path: "/captcha_solve" }, { label: "Cookie管理", path: "/cookie_jar" }, { label: "页面渲染", path: "/page_render" }, { label: "抽取规则", path: "/extract_rule" }, { label: "种子URL", path: "/seed_url" }, { label: "内容去重", path: "/content_dedup" }, { label: "结果解析", path: "/result_parse" }, { label: "Schema匹配", path: "/schema_match" }, { label: "调度计划", path: "/scheduler" }, { label: "数据导出", path: "/export_dataset" }, { label: "质量抽检", path: "/quality_check" }, { label: "机器人校验", path: "/robot_verify" }, { label: "投递管道", path: "/delivery_pipe" }, { label: "弹性伸缩", path: "/scalable_pool" }]
</script>
