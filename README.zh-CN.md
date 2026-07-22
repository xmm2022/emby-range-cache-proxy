# Emby Range Cache Proxy

Emby 原盘直放请求的本地 Range 缓存代理。生产环境建议使用 Go 版本；Python 版本保留为兼容实现和回滚路径。

## 与 MediaInfoKeeper 联动

本服务可以与 [MediaInfoKeeper](https://github.com/xmm2022/MediaInfoKeeper) 配合使用，但两个项目保持独立部署：

- MediaInfoKeeper 运行在 Emby 进程内，把新入库、快捷菜单提取、计划任务提取和播放下一集等事件转换为明确的预热请求。
- Emby Range Cache Proxy 是独立的 Go 数据面，负责缓存资格校验、源站访问、head/tail 与可选 middle cache，并执行持久化的 `normal`、`read_only`、`bypass` 模式。
- Caddy 或其他可信反向代理负责验证播放签名，只对合格播放路由设置 `X-Range-Cache-Eligible: 1`。普通 STRM、ffprobe、截图和其他探测请求不得携带该资格头。

两个服务不需要位于同一台主机。分离部署时，插件建议只访问 Emby 主机上的 loopback 控制桥；控制桥再通过 TLS、来源 IP 白名单和独立控制密钥访问远端 Go 服务。

```text
Emby + MediaInfoKeeper
  ├─ itemId/mediaSourceId ──> /internal/prewarm
  └─ Range Cache 总开关 ──> 本机控制桥 ──> /internal/cache-mode

客户端播放 ──> 可信签名反向代理 ──> Range Cache Proxy ──> 源站
```

MediaInfo 提取本身不等于缓存资格。MediaInfoKeeper 的详情页提取和 MediaInfo 预加载不会隐式预热本服务；插件侧三个 Range Cache 开关的优先级和作用见 [MediaInfoKeeper README](https://github.com/xmm2022/MediaInfoKeeper#readme)。

## 当前版本适合怎么用

这个版本已经可以作为独立服务部署，控制入口主要分三层：

- 服务控制：用 systemd 或 Docker Compose 启停 Go 服务。
- 流量控制：用 Caddy 只把指定影片、指定 `MediaSourceId` 的原盘直放请求转到缓存代理。
- 功能控制：用 `/etc/emby-range-cache-proxy/config.json` 开关预热、会话观察、中段缓存、预取、白名单和缓存容量；用 `/internal/cache-mode` 热切换总运行模式。

需要注意：普通 JSON 配置仍在启动时加载，修改后需要重启服务；只有 Range Cache 总运行模式通过内部接口热切换并写入 SQLite，服务重启后仍会保留。当前没有 Web 管理界面。

## 推荐部署方式

优先使用 Go 版本：

```bash
cd /opt/emby-range-cache-proxy
make build
make check-config CONFIG=/etc/emby-range-cache-proxy/config.json
./go/bin/emby-range-cache-proxy --config /etc/emby-range-cache-proxy/config.json --print-effective-config
```

`--print-effective-config` 会输出合并默认值后的最终 JSON 配置，适合部署前核对实际生效参数。默认会把 `prewarm_api_key` 和 OpenList 密钥显示为 `REDACTED`；只有明确需要检查密钥明文时才加 `--show-secrets`。

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
  "control_api_key": "replace-with-a-different-long-random-secret",
  "playback_info_timeout_seconds": 15,
  "rollout": {
    "enabled": true,
    "item_allowlist": ["10535"],
    "media_source_allowlist": ["mediasource_10535"],
    "path_prefix_allowlist": ["http://127.0.0.1:18096/"]
  }
}
```

`listen_host` 建议保持 `127.0.0.1`，不要把服务直接暴露到公网。`path_prefix_allowlist` 建议写完整 URL 前缀并带尾部 `/`，例如 `http://127.0.0.1:18096/`。

如果媒体源走 OpenList，配置 `openlist.enabled=true`、`openlist.base_url`，需要鉴权时再填 `openlist.token`。`.strm` 里可以写 `openlist:///Movies/movie.mkv`，并把 OpenList 地址（例如 `http://127.0.0.1:5244/`）加入 `rollout.path_prefix_allowlist`。

## 直接边缘路径

Go 服务还可以直接处理稳定的媒体路径，避免每个媒体 Range 请求都再次查询 Emby `PlaybackInfo`：

- `direct_openlist` 把受控路径前缀映射到 OpenList，通过 `/api/fs/get` 刷新下载地址并使用返回的可信文件大小，然后进入统一的 head/tail 和中段缓存流程。
- `direct_http` 把受控路径前缀映射到固定 HTTP(S) 上游，适合 Google API Proxy 或其他内网源站。

两个功能默认关闭。服务仍应监听 loopback，或放在带鉴权/签名验证的反向代理之后；直接路径本身不等价于用户鉴权。

```json
{
  "direct_openlist": {
    "enabled": true,
    "path_prefix": "/openlist/",
    "token": "replace-with-a-long-random-secret"
  },
  "direct_http": {
    "enabled": true,
    "path_prefix": "/google/",
    "upstream_base_url": "http://127.0.0.1:18096"
  }
}
```

当容器元数据读取以文件末尾结束、但起点早于固定 tail 块时，`cache.adaptive_tail_max_bytes` 可以扩大尾部缓存；设置为 `0` 即关闭。`open_head_response_bytes_by_extension` 与 `open_initial_response_bytes_by_extension` 可以按容器扩展名调整起播响应大小。

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

## Metrics

`GET /internal/metrics` 是 loopback-only 的 Prometheus 文本指标接口，包含 `/internal/stats` 里的主要运行状态：

- cache hit/build/origin/fallback/deny/proxy error 计数。
- prewarm queued/running/completed/skipped。
- prefetch queue/running/done/failed。
- head/tail 缓存字节数、中段缓存字节数、磁盘可用空间。
- rollout、session、middle cache、prefetch、prewarm 等配置开关状态。

```bash
curl -fsS http://127.0.0.1:18180/internal/metrics
```

不要把 `/internal/metrics` 暴露到公网反代。建议只让本机 Prometheus agent、私有管理网络或 SSH 隧道访问。

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

不要把 `/internal/prewarm` 暴露到公网反代。服务会拒绝非 loopback 调用；如果 loopback 反代请求带有非 loopback 的 `X-Forwarded-For` 或 `X-Real-IP`，也会拒绝。内部接口只应该给本机插件或本机脚本使用。

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

[MediaInfoKeeper](https://github.com/xmm2022/MediaInfoKeeper) 可以按插件开关在入库、快捷菜单、计划任务或播放下一集时调用这个接口；缓存代理本身不依赖 Emby 插件，也可以由脚本、定时任务或其他控制面调用。

## 运行时总模式

`GET /internal/cache-mode` 返回持久化的当前模式。`POST` 接受 `normal`、`read_only` 或 `bypass`，并要求 loopback 调用者携带 `X-Range-Cache-Control-Key: <control_api_key>`。模式写入状态 SQLite，服务重启后仍然生效。

- `normal`：读取已有缓存，并允许新建缓存、预热、会话记录和后台预取。
- `read_only`：可以读取已有 head/tail/middle；缓存未命中时直接回源，不建立新缓存或会话。
- `bypass`：完全忽略已有缓存。直接路由回源，普通 Emby 路由回退到 Emby，已鉴权预热返回 `409`。

控制接口如需跨主机暴露，必须置于 TLS 反向代理之后，并限制来源 IP。`control_api_key` 不应复用 `prewarm_api_key`。

## 常用配置项

| 配置项 | 作用 | 建议 |
| --- | --- | --- |
| `emby_base_url` | Emby 内网地址，用于鉴权和查询 PlaybackInfo | 通常为 `http://127.0.0.1:8096` |
| `fallback_base_url` | 不命中缓存代理时回源到 Emby 的地址 | 通常同 `emby_base_url` |
| `listen_host` / `listen_port` | 缓存代理监听地址 | 建议 `127.0.0.1:18180` |
| `cache_dir` | head/tail/middle 缓存和状态库目录 | 放在容量足够的磁盘 |
| `prewarm_api_key` | 内部预热和后台任务密钥 | 使用长随机值，不要复用 Emby 用户 token |
| `control_api_key` | `/internal/cache-mode` 的独立控制密钥 | 不要与 `prewarm_api_key` 复用 |
| `playback_info_timeout_seconds` | 用户播放请求查询 Emby PlaybackInfo 的前台鉴权超时 | 默认 `15`，冷启动慢可调大 |
| `openlist.enabled` / `openlist.base_url` | 启用 OpenList 源适配并配置 OpenList 地址 | `.strm` 可写 `openlist:///Movies/movie.mkv` |
| `openlist.token` | 调用 OpenList `/api/fs/get` 的 token | 按 OpenList 原样填，不要加 `Bearer` |
| `direct_openlist.enabled` / `direct_openlist.path_prefix` | 直接处理受控 OpenList 路径 | 必须置于可信反代之后 |
| `direct_http.enabled` / `direct_http.upstream_base_url` | 直接处理受控 HTTP 路径并映射到固定上游 | 适合 Google API Proxy 等内网源站 |
| `rollout.enabled` | 是否启用缓存代理命中范围 | 灰度时设为 `true` |
| `rollout.item_allowlist` | 只允许指定 item 进入代理逻辑 | 初期只放一两个影片 |
| `rollout.media_source_allowlist` | 只允许指定 MediaSource | 避免同影片多源误命中 |
| `rollout.path_prefix_allowlist` | 限制实际源 URL 前缀 | `.strm` 场景必须配置严谨 |
| `cache.max_bytes` | head/tail 缓存上限 | 按磁盘容量设置 |
| `cache.head_bytes` / `cache.tail_bytes` | 起播头部块和尾部元数据块大小 | 默认各 `8388608` |
| `cache.adaptive_tail_max_bytes` | 文件尾部元数据跨出固定 tail 时允许扩展的最大缓存 | `0` 表示关闭，启用值不能小于 `tail_bytes` |
| `cache.open_head_response_bytes_by_extension` | 按扩展名设置无结束位置 Range 的响应大小 | 键名可写 `mp4` 或 `.mp4` |
| `cache.open_initial_response_bytes_by_extension` | 按扩展名设置从 0 开始的开放 Range 响应大小 | 用于特定容器起播调优 |
| `prewarm.concurrency` | 内部预热并发 | 建议从 `1` 开始 |
| `prewarm.playback_info_timeout_seconds` | 内部预热查询 Emby PlaybackInfo 的超时 | 默认 `15`，与前台播放鉴权超时分开 |
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
- OpenList 适配会调用 `/api/fs/get` 刷新文件 `sign`，并以 OpenList `/d/...?...sign` 作为回源地址；使用时把 OpenList 地址（如 `http://127.0.0.1:5244/`）加入 `rollout.path_prefix_allowlist`。

## 开发验证

```bash
python3 -m pip install -e ".[dev]"
python3 -m pytest -q
make test-go
make build
make check-config CONFIG=config.example.json
./go/bin/emby-range-cache-proxy --config config.example.json --print-effective-config
go test -race ./...
```
