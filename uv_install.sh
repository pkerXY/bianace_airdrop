#!/bin/bash

set -e

echo "=== 安装系统构建依赖 ==="
sudo apt update
sudo apt install -y \
    build-essential \
    curl \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev

echo "=== 安装 uv ==="
curl -LsSf https://astral.sh/uv/install.sh | sh

echo "=== 将 uv 加入 PATH（如果尚未加入） ==="
if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' ~/.bashrc; then
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    echo "已写入 ~/.bashrc"
fi

# 让 PATH 立即生效
export PATH="$HOME/.local/bin:$PATH"

echo "=== 使用 uv 安装最新稳定 Python （比如 3.12 或 3.13）==="
uv python install 3.12.3

echo "=== 设置默认 Python 使用 uv 安装的版本 ==="
uv python pin

echo
echo "=== 完成！===

你现在可以执行：
    source ~/.bashrc 让 PATH 设置立刻生效
    uv python list
查看已安装的 Python 版本
