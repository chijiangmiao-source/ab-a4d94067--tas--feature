FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=0

WORKDIR /srv

# 纯标准库实现，无第三方依赖；直接拷贝应用、测试与验收脚本。
COPY app ./app
COPY tests ./tests
COPY scripts ./scripts
RUN chmod +x scripts/verify.sh scripts/http_smoke.py \
    && python3 -m compileall -q app scripts

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=10 \
    CMD python3 -c "import json,urllib.request,sys; sys.exit(0 if json.load(urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=3)).get('ok') else 1)"

CMD ["python3", "-m", "app.server", "--host", "0.0.0.0", "--port", "8080"]
