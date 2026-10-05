#!/usr/bin/env sh
# 一次性验收：构建检查 -> 代码测试 -> 审计接口 HTTP 冒烟。
# 全部通过退出码 0，任一失败立即以非零码退出（供 Compose verify 服务使用）。
set -eu

cd "$(dirname "$0")/.."

echo "================ [1/3] 构建检查（语法编译） ================"
python3 -m compileall -q app scripts
echo "构建检查通过。"

echo "================ [2/3] 代码测试（unittest） ================"
python3 -m unittest discover -s tests -v

echo "================ [3/3] 审计接口 HTTP 冒烟 ================"
# 若 Compose 中 web 服务已在线则直连；否则脚本会自行起服。
if [ -n "${SMOKE_BASE_URL:-}" ]; then
  python3 scripts/http_smoke.py "$SMOKE_BASE_URL"
else
  python3 scripts/http_smoke.py
fi

echo "================ verify 全部通过 ================"
