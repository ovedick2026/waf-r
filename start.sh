#!/bin/sh
set -e

echo "[start.sh] 容器启动..."

# ---- 1. 启动 cloudflared 隧道(后台)----
if [ -n "$CLOUDFLARE_TUNNEL_TOKEN" ]; then
    echo "[start.sh] 检测到 Tunnel Token,启动 cloudflared..."
    cloudflared tunnel --no-autoupdate run --token "$CLOUDFLARE_TUNNEL_TOKEN" &
    CF_PID=$!
    echo "[start.sh] cloudflared 已在后台启动 (pid=$CF_PID)"
else
    echo "[start.sh] 警告:未设置 CLOUDFLARE_TUNNEL_TOKEN,跳过隧道(服务将无法通过域名访问)"
fi

# ---- 2. 启动 cctool 主程序(前台)----
# 直接调用官方镜像里的入口程序,让 cctool 成为容器主进程
echo "[start.sh] 启动 cctool..."

npm start
