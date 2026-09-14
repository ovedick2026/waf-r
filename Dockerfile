FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm install --only=production

COPY . .

# 安装 cloudflared
# 自动识别 CPU 架构
USER root
RUN set -eux; \
    arch="$(uname -m)"; \
    case "$arch" in \
        x86_64)  cf_arch="amd64" ;; \
        aarch64) cf_arch="arm64" ;; \
        *) echo "unsupported arch: $arch"; exit 1 ;; \
    esac; \
    if command -v apt-get >/dev/null 2>&1; then \
        apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*; \
    elif command -v apk >/dev/null 2>&1; then \
        apk add --no-cache curl ca-certificates; \
    fi; \
    curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${cf_arch}" \
        -o /usr/local/bin/cloudflared; \
    chmod +x /usr/local/bin/cloudflared; \
    cloudflared --version

# 放入启动脚本
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 7860

ENTRYPOINT ["/start.sh"]
