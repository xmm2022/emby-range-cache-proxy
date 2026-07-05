# Emby Range Cache Proxy

Emby 原盘直放请求的本地 Range 缓存代理。生产环境建议使用 Go 版本；Python 版本保留为兼容实现和回滚路径。

## 当前版本适合怎么用

这个版本已经可以作为独立服务部署，控制入口主要分三层：

- 服务控制：用 systemd 或 Docker Compose 启停 Go 服务。
- 流量控制：用 Caddy 只把指定影片、指定 `MediaSourceId` 的原盘直放请求转到缓存代理。
- 功能控制：用 `/etc/emby-range-cache-proxy/config.json` 开关预热、会话观察、中段缓存、预取、白名单和缓存容量。

需要注意：当前配置是启动时加载的 JSON 配置，修改后需要重启服务生效；还没有 Web 管理界面，也没有热加载接口。对服务器部署来说这样更简单、可审计；如果后续要频繁远程调整，可以再加一个本机管理 API 或管理面板。

## 推荐部署方式

优先使用 Go 版本：

```bash
cd /opt/emby-range-cache-proxy
make build
make check-config CONFIG=/etc/emby-range-cache-proxy/config.json
```

配置文件建议放在：

```bash
/etc/emby-range-cache-proxy/config.json
```

最小配置示例：

```json
{
  "emby_base_url": "http://127.0.0.1:8096",
  "fallback_base_url": "http://127.0.0.1:8096",
  "listen_host": "127.0.0.1",
  "listen_port": 18180,
  "cache_dir": "/home/nax/emby/cache/range-proxy",
  "prewarm_api_key": "replace-with-a-long-random-secret",
  "rollout": {
    "enabled": true,
    "item_allowlist": ["10535"],
    "media_source_allowlist": ["mediasource_10535"],
    "path_prefix_allowlist": ["http://127.0.0.1:18096/"]
  }
}
```

`listen_host` 建议保持 `127.0.0.1`，不要把服务直接暴露到公网。`path_prefix_allowlist` 建议写完整 URL 前缀并带尾部 `/`，例如 `http://127.0.0.1:18096/`。

## systemd 部署

先创建运行用户和缓存目录：

```bash
useradd --system --home /nonexistent --shell /usr/sbin/nologin emby-cache || true
install -d -o emby-cache -g emby-cache /home/nax/emby/cache/range-proxy
```

构建并安装服务：

```bash
make build
install -m 0644 go/deploy/emby-range-cache-proxy-go.service /etc/systemd/system/emby-range-cache-proxy.service
systemctl daemon-reload
systemctl restart emby-range-cache-proxy.service
systemctl status emby-range-cache-proxy.service --no-pager
curl -fsS http://127.0.0.1:18180/healthz
curl -fsS http://127.0.0.1:18180/internal/stats
```

回滚到 Python 前建议先备份原 unit：

```bash
systemctl cat emby-range-cache-proxy.service > /root/emby-range-cache-proxy-python-unit.backup
```

## Docker Compose 部署

示例 Compose 使用 host network，这样容器里的 `127.0.0.1:8096` 仍然指向宿主机上的 Emby。

```bash
install -d /etc/emby-range-cache-proxy /home/nax/emby/cache/range-proxy
cp config.example.json /etc/emby-range-cache-proxy/config.json
# 编辑 /etc/emby-range-cache-proxy/config.json
docker compose -f docker-compose.example.yml build
docker compose -f docker-compose.example.yml up -d
curl -fsS http://127.0.0.1:18180/healthz
```

容器默认使用 UID `10001` 运行。如果缓存目录无法写入，调整目录权限或修改 `docker-compose.example.yml` 里的 volume 路径。

## Docker Hub 镜像

发布镜像后可以这样运行：

```bash
docker run -d \
  --name emby-range-cache-proxy \
  --network host \
  --restart unless-stopped \
  -v /etc/emby-range-cache-proxy/config.json:/config/config.json:ro \
  -v /home/nax/emby/cache/range-proxy:/home/nax/emby/cache/range-proxy \
  xmm2022/emby-range-cache-proxy:0.1.0 \
  --config /config/config.json
```

也可以使用 `latest`，但生产环境更建议固定版本号。

## Caddy 分流示例

只把命中的原盘直放请求转到缓存代理，其他请求继续走 Emby：

```caddyfile
@unified_range_proxy_10535 {
	path /emby/videos/10535/original.mkv
	query MediaSourceId=mediasource_10535
}

handle @unified_range_proxy_10535 {
	reverse_proxy 127.0.0.1:18180 127.0.0.1:8096 {
		lb_policy first
		lb_try_duration 2s
		lb_try_interval 100ms
		fail_duration 10s
		flush_interval -1
	}
}

handle {
	reverse_proxy 127.0.0.1:8096 {
		flush_interval -1
	}
}
```

不要把 `/internal/prewarm` 暴露到公网反代。服务会拒绝非 loopback 调用，内部接口只应该给本机插件或本机脚本使用。

## 内部预热接口

事件触发预热不需要 `prewarm.enabled=true`，只需要配置 `prewarm_api_key`。

```bash
curl -fsS -X POST http://127.0.0.1:18180/internal/prewarm \
  -H 'Content-Type: application/json' \
  -H "X-Range-Cache-Prewarm-Key: ${RANGE_CACHE_PREWARM_KEY}" \
  --data '{"itemId":"10535","mediaSourceId":"mediasource_10535"}'
```

返回含义：

- `queued`：新任务已进入队列。
- `existing`：同一个 item/source 已经在排队或运行。

当前预热触发可以由你魔改过的 Emby 插件在媒体信息提取完成后调用这个本机接口；缓存代理本身不依赖 Emby 插件，也可以用脚本、定时任务或其他控制面调用。

## 常用配置项

| 配置项 | 作用 | 建议 |
| --- | --- | --- |
| `emby_base_url` | Emby 内网地址，用于鉴权和查询 PlaybackInfo | 通常为 `http://127.0.0.1:8096` |
| `fallback_base_url` | 不命中缓存代理时回源到 Emby 的地址 | 通常同 `emby_base_url` |
| `listen_host` / `listen_port` | 缓存代理监听地址 | 建议 `127.0.0.1:18180` |
| `cache_dir` | head/tail/middle 缓存和状态库目录 | 放在容量足够的磁盘 |
| `prewarm_api_key` | 内部预热和后台任务密钥 | 使用长随机值，不要复用 Emby 用户 token |
| `rollout.enabled` | 是否启用缓存代理命中范围 | 灰度时设为 `true` |
| `rollout.item_allowlist` | 只允许指定 item 进入代理逻辑 | 初期只放一两个影片 |
| `rollout.media_source_allowlist` | 只允许指定 MediaSource | 避免同影片多源误命中 |
| `rollout.path_prefix_allowlist` | 限制实际源 URL 前缀 | `.strm` 场景必须配置严谨 |
| `cache.max_bytes` | head/tail 缓存上限 | 按磁盘容量设置 |
| `prewarm.concurrency` | 内部预热并发 | 建议从 `1` 开始 |
| `session.enabled` | 记录播放会话 | Phase 2 功能，默认关闭 |
| `middle_cache.enabled` | 启用中段缓存 | 确认稳定后再开 |
| `prefetch.enabled` | 启用空闲/停止后的中段预取 | 最后灰度开启 |
| `prefetch.bandwidth_bytes_per_second` | 后台预取限速 | 避免影响正常播放 |

## 推荐灰度顺序

1. 只部署服务，保持 Phase 2 关闭。
2. 给一个 item 配置 `rollout.item_allowlist` 和 `media_source_allowlist`。
3. 用 Caddy 只转发这个 item 的原盘请求。
4. 配置 `prewarm_api_key`，让插件或脚本调用 `/internal/prewarm`。
5. 确认稳定后开启 `session.enabled=true`。
6. 再开启 `middle_cache.enabled=true`。
7. 最后对少量 item 开启 `prefetch.enabled=true`。

## 安全边界

- 用户播放请求仍然用用户自己的 Emby token 做 `PlaybackInfo` 鉴权。
- 内部预热密钥只用于后台查询和预热，不替代用户鉴权。
- 内部接口只接受 loopback 来源。
- 日志不应包含用户 token、`api_key`、`X-Emby-Token`、`PlaySessionId`、`DeviceId`、源站 URL 或完整 query。
- `.strm` 本地路径读取只允许配置过的 `path_mappings`，解析出来的 URL 还要命中 `rollout.path_prefix_allowlist`。

## 开发验证

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
make test-go
make build
make check-config CONFIG=config.example.json
go test -race ./...
```
