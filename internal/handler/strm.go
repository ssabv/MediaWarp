package handler

import (
	"MediaWarp/internal/config"
	"MediaWarp/internal/logging"
	"MediaWarp/internal/service"
	"context"
	"fmt"
	"net/http"

	"github.com/allegro/bigcache/v3"
)

type StrmHandlerFunc func(content string, ua string) string

// prefetchCache 预提取专用长 TTL 缓存。
// 预提取/定时刷新得到的直链写入此处,TTL 由 prefetch.prefetch_cache_ttl 控制(默认 2h),
// 播放中定时刷新会持续回填续期,保证预提取结果活到下一次提取;
// 停止播放后按 TTL 自动过期,避免长期缓存已失效的直链。
var prefetchCache *bigcache.BigCache

func getHTTPStrmHandler() (StrmHandlerFunc, error) {
	var cache *bigcache.BigCache
	if config.Cache.Enable && config.Cache.HTTPStrmTTL > 0 && config.HTTPStrm.FinalURL {
		var err error
		cache, err = bigcache.New(context.Background(), bigcache.DefaultConfig(config.Cache.HTTPStrmTTL))
		if err != nil {
			return nil, fmt.Errorf("创建 HTTPStrm 缓存失败: %w", err)
		}
		logging.Info("启用 HTTPStrm 缓存，TTL: ", config.Cache.HTTPStrmTTL)
	}

	// 预提取专用缓存:TTL 默认 2h,与 http_strm_ttl 解耦
	if config.Prefetch.Enable {
		var err error
		prefetchCache, err = bigcache.New(context.Background(), bigcache.DefaultConfig(config.Prefetch.PrefetchCacheTTL))
		if err != nil {
			return nil, fmt.Errorf("创建预提取缓存失败: %w", err)
		}
		logging.Info("启用预提取长 TTL 缓存，TTL: ", config.Prefetch.PrefetchCacheTTL)
	}

	client := &http.Client{ // 创建自定义HTTP客户端配置
		Timeout: RedirectTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// 禁止自动重定向，以便手动处理
			return http.ErrUseLastResponse
		},
	}
	return func(content string, ua string) string {
		if config.HTTPStrm.FinalURL {
			if cache != nil {
				if cachedURL, err := cache.Get(content); err == nil {
					// 命中缓存后重新写入,重置 TTL 以延长存活期到下一次提取,
					// 保证预提取的直链不会在播放中途因 TTL 到期而失效
					if err := cache.Set(content, cachedURL); err != nil {
						logging.Warning("续期 HTTPStrm 缓存失败: ", err)
					}
					logging.Infof("HTTPStrm 重定向至: %s (缓存)", string(cachedURL))
					return string(cachedURL)
				}
			}

			// 二级兜底:查预提取长 TTL 缓存,命中则回填常规缓存并返回
			if prefetchCache != nil {
				if cachedURL, err := prefetchCache.Get(content); err == nil {
					if cache != nil {
						if err := cache.Set(content, cachedURL); err != nil {
							logging.Warning("回填 HTTPStrm 缓存失败: ", err)
						}
					}
					logging.Infof("HTTPStrm 重定向至: %s (预取缓存)", string(cachedURL))
					return string(cachedURL)
				}
			}

			logging.Debug("HTTPStrm 启用获取最终 URL，开始尝试获取最终 URL")
			finalURL, err := getFinalURL(client, content, ua)
			if err != nil {
				logging.Warning("获取最终 URL 失败，使用原始 URL: ", err)
			} else {
				logging.Info("HTTPStrm 重定向至: ", finalURL)
			}
			if cache != nil {
				if err := cache.Set(content, []byte(finalURL)); err != nil {
					logging.Warning("缓存 HTTPStrm URL 失败: ", err)
				} else {
					logging.Debug("缓存 HTTPStrm URL 成功")
				}
			}
			if prefetchCache != nil {
				if err := prefetchCache.Set(content, []byte(finalURL)); err != nil {
					logging.Warning("缓存预提取 URL 失败: ", err)
				}
			}
			return finalURL
		} else {
			logging.Debug("HTTPStrm 未启用获取最终 URL，直接使用原始 URL: ", content)
			return content
		}
	}, nil
}

func alistStrmHandler(content string, alistAddr string) string {
	alistClient, err := service.GetAlistClient(alistAddr)
	if err != nil {
		logging.Warning("获取 AlistClient 失败：", err)
		return ""
	}
	url, err := alistClient.GetFileURL(content, config.AlistStrm.RawURL)
	if err != nil {
		logging.Warning("获取文件 URL 失败：", err)
		return ""
	}
	logging.Infof("AlistStrm 重定向至：%s", url)
	return url
}
