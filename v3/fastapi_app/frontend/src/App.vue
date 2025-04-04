<template>
  <div class="d-flex">
    <div class="sidebar bg-dark text-white p-3" style="width: 250px; min-height: 100vh">
      <h4 class="text-center mb-4">LogVision</h4>
      <ul class="nav flex-column">
        <li class="nav-item">
          <router-link class="nav-link text-white" to="/">首页</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link text-white" to="/logs">实时日志</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link text-white" to="/analytics">系统状态</router-link>
        </li>
        <li class="nav-item">
          <router-link class="nav-link text-white" to="/analytics">系统设置</router-link>
        </li>
      </ul>
    </div>
    <div class="main-content p-4" style="flex: 1">
      <router-view />
    </div>
  </div>
</template>

<script setup>
import 'bootstrap/dist/css/bootstrap.min.css'
import 'bootstrap'
</script>

<style scoped>
.sidebar {
  position: fixed;
}
.main-content {
  margin-left: 250px;
}
</style>