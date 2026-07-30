package handler

import (
	"MediaWarp/constants"
	"MediaWarp/internal/config"
	"MediaWarp/internal/logging"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/tidwall/gjson"
)

// playbackTimer 播放定时器，用于定时刷新预提取缓存
type playbackTimer struct {
	ticker    *time.Ticker
	stopChan  chan struct{}
	nextID    string // 下一集 ID
	runTimeSec int64  // 当前集时长（秒）
}

// PrefetchService 下一集直链预提取服务
type PrefetchService struct {
	cache             *bigcache.BigCache
	httpStrmHandler   StrmHandlerFunc
	mediaServerType   constants.MediaServerType
	mediaServerAddr   string
	mediaServerAPIKey string

	mu         sync.Mutex
	timers     map[string]*playbackTimer // map[currentEpisodeID]*playbackTimer

	// 并发控制
	sem chan struct{}
}

var prefetchService *PrefetchService

// GetPrefetchService 获取预提取服务实例
func GetPrefetchService() *PrefetchService {
	return prefetchService
}

// InitPrefetchService 初始化预提取服务
func InitPrefetchService(mediaServerType constants.MediaServerType, addr string, apiKey string) error {
	if !config.Prefetch.Enable {
		logging.Info("预提取服务未启用")
		return nil
	}

	var cache *bigcache.BigCache
	var err error
	if config.Cache.Enable {
		cache, err = bigcache.New(
			context.Background(),
			bigcache.DefaultConfig(30*time.Minute),
		)
		if err != nil {
			return fmt.Errorf("创建预提取缓存失败: %w", err)
		}
	}

	httpStrmHandler, err := getHTTPStrmHandler()
	if err != nil {
		return fmt.Errorf("预提取服务创建 HTTPStrm 处理器失败: %w", err)
	}

	maxConcurrent := config.Prefetch.MaxConcurrentPrefetch
	if maxConcurrent <= 0 {
		maxConcurrent = 3
	}

	prefetchService = &PrefetchService{
		cache:             cache,
		httpStrmHandler:   httpStrmHandler,
		mediaServerType:   mediaServerType,
		mediaServerAddr:   addr,
		mediaServerAPIKey: apiKey,
		timers:            make(map[string]*playbackTimer),
		sem:               make(chan struct{}, maxConcurrent),
	}

	logging.Infof("预提取服务已启用（定时刷新模式，刷新间隔: %.0f%% 视频时长，最大并发: %d）",
		config.Prefetch.RefreshInterval*100,
		maxConcurrent,
	)
	return nil
}

// OnPlaybackStart 播放开始时触发，查询下一集并启动定时刷新
func (p *PrefetchService) OnPlaybackStart(itemID string) {
	if !config.Prefetch.Enable {
		return
	}

	logging.Infof("播放开始: %s，查询下一集...", itemID)

	// 查询当前剧集的运行时信息
	runTimeTicks, seriesID, seasonID, currentIndex, err := p.queryItemRuntime(itemID)
	if err != nil {
		logging.Debugf("预提取：查询剧集信息失败: %v", err)
		return
	}
	if runTimeTicks <= 0 {
		logging.Debugf("预提取：剧集 %s 运行时为 0，跳过", itemID)
		return
	}

	runTimeSec := runTimeTicks / 10000000 // ticks → 秒

	// 查询下一集
	if seriesID == "" || seasonID == "" || currentIndex < 0 {
		logging.Debugf("预提取：剧集 %s 缺少 SeriesID/SeasonID/IndexNumber，无法获取下一集", itemID)
		return
	}

	nextItemID, err := p.queryNextEpisodeID(seriesID, seasonID, currentIndex)
	if err != nil {
		logging.Debugf("预提取：查询下一集失败: %v", err)
		return
	}
	if nextItemID == "" {
		logging.Debugf("预提取：剧集 S%sE%d 没有下一集", seasonID, currentIndex)
		return
	}

	logging.Infof("当前剧集: %s (时长: %d秒), 下一集: %s", itemID, runTimeSec, nextItemID)

	// 立即预取一次
	go p.doPrefetch(nextItemID)

	// 停止旧定时器
	p.stopTimer(itemID)

	// 计算刷新间隔：每 25% 视频时长刷新一次
	refreshInterval := float64(runTimeSec) * config.Prefetch.RefreshInterval
	if refreshInterval < 30 {
		refreshInterval = 30 // 最短 30 秒
	}
	interval := time.Duration(refreshInterval) * time.Second

	timer := &playbackTimer{
		ticker:    time.NewTicker(interval),
		stopChan:  make(chan struct{}),
		nextID:    nextItemID,
		runTimeSec: runTimeSec,
	}

	p.mu.Lock()
	p.timers[itemID] = timer
	p.mu.Unlock()

	logging.Infof("预提取：启动定时器，每 %v 刷新一次下一集 %s 的直链（%.0f 分钟视频，每 %.0f 分钟刷新）",
		interval, nextItemID,
		float64(runTimeSec)/60.0,
		interval.Seconds()/60.0,
	)

	// 启动定时刷新协程
	go func() {
		for {
			select {
			case <-timer.ticker.C:
				logging.Infof("预提取：定时刷新下一集 %s 的直链", timer.nextID)
				go p.doPrefetch(timer.nextID)
			case <-timer.stopChan:
				logging.Infof("预提取：停止定时器，剧集 %s 播放结束", itemID)
				timer.ticker.Stop()
				return
			}
		}
	}()
}

// OnPlaybackStop 播放停止时取消定时器
func (p *PrefetchService) OnPlaybackStop(itemID string) {
	p.stopTimer(itemID)
}

func (p *PrefetchService) stopTimer(itemID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if timer, ok := p.timers[itemID]; ok {
		close(timer.stopChan)
		delete(p.timers, itemID)
	}
}

// doPrefetch 执行预提取（带并发控制）
func (p *PrefetchService) doPrefetch(itemID string) {
	// 并发控制
	p.sem <- struct{}{}
	defer func() { <-p.sem }()

	// 检查是否已有未过期的缓存
	if cached := p.GetCachedURL(itemID); cached != "" {
		logging.Debugf("预提取：剧集 %s 已有有效缓存，跳过", itemID)
		return
	}

	logging.Infof("预提取：开始解析剧集 %s 的直链", itemID)

	// 获取 Strm 内容（HTTP URL）和类型
	content, ua, err := p.getStrmContent(itemID)
	if err != nil {
		logging.Warningf("预提取：获取剧集 %s 的 Strm 内容失败: %v", itemID, err)
		return
	}
	if content == "" {
		logging.Debugf("预提取：剧集 %s 不是 HTTPStrm，跳过", itemID)
		return
	}

	// 调用 httpStrmHandler 获取最终直链
	finalURL := p.httpStrmHandler(content, ua)
	if finalURL == "" {
		logging.Warningf("预提取：解析剧集 %s 直链失败", itemID)
		return
	}

	// 解析过期时间（从 URL 签名推断）
	expiresAt := p.extractExpiresFromURL(finalURL)

	// 写入缓存
	if p.cache != nil {
		cacheValue := fmt.Sprintf("%s|%d|%d",
			finalURL,
			expiresAt.Unix(),
			time.Now().Unix(),
		)
		if err := p.cache.Set(itemID, []byte(cacheValue)); err != nil {
			logging.Warningf("预提取：缓存剧集 %s 直链失败: %v", itemID, err)
			return
		}

		ttl := expiresAt.Sub(time.Now())
		logging.Infof("预提取：剧集 %s 直链已缓存，TTL: %v", itemID, ttl.Round(time.Second))
	}
}

// queryItemRuntime 查询剧集的运行时和剧集信息
func (p *PrefetchService) queryItemRuntime(itemID string) (runTimeTicks int64, seriesID string, seasonID string, indexNumber int64, err error) {
	params := url.Values{}
	params.Add("Ids", itemID)
	params.Add("Limit", "1")
	params.Add("Fields", "RunTimeTicks,SeriesId,SeasonId,IndexNumber")

	var apiURL string
	switch p.mediaServerType {
	case constants.EMBY:
		params.Add("api_key", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/emby/Items?%s", p.mediaServerAddr, params.Encode())
	case constants.JELLYFIN:
		params.Add("ApiKey", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/Items?%s", p.mediaServerAddr, params.Encode())
	default:
		return 0, "", "", -1, fmt.Errorf("不支持的媒体服务器类型")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return 0, "", "", -1, err
	}
	defer resp.Body.Close()

	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	bodyStr := string(body[:n])

	runTimeTicks = gjson.Get(bodyStr, "Items.0.RunTimeTicks").Int()
	seriesID = gjson.Get(bodyStr, "Items.0.SeriesId").String()
	seasonID = gjson.Get(bodyStr, "Items.0.SeasonId").String()
	indexNumber = gjson.Get(bodyStr, "Items.0.IndexNumber").Int()

	return runTimeTicks, seriesID, seasonID, indexNumber, nil
}

// queryNextEpisodeID 查询下一集 ID
func (p *PrefetchService) queryNextEpisodeID(seriesID string, seasonID string, currentIndex int64) (string, error) {
	nextIndex := currentIndex + 1

	params := url.Values{}
	params.Add("ParentId", seasonID)
	params.Add("IncludeItemTypes", "Episode")
	params.Add("Fields", "IndexNumber")
	params.Add("SortBy", "SortName")
	params.Add("SortOrder", "Ascending")
	params.Add("Limit", "100")

	var apiURL string
	switch p.mediaServerType {
	case constants.EMBY:
		params.Add("api_key", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/emby/Items?%s", p.mediaServerAddr, params.Encode())
	case constants.JELLYFIN:
		params.Add("ApiKey", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/Items?%s", p.mediaServerAddr, params.Encode())
	default:
		return "", fmt.Errorf("不支持的媒体服务器类型")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body := make([]byte, 16384)
	n, _ := resp.Body.Read(body)
	bodyStr := string(body[:n])

	items := gjson.Get(bodyStr, "Items")
	for _, item := range items.Array() {
		if item.Get("IndexNumber").Int() == nextIndex {
			return item.Get("Id").String(), nil
		}
	}

	return "", nil
}

// getStrmContent 获取剧集的 Strm 文件内容和识别类型
func (p *PrefetchService) getStrmContent(itemID string) (content string, ua string, err error) {
	params := url.Values{}
	params.Add("Ids", itemID)
	params.Add("Limit", "1")
	params.Add("Fields", "Path,MediaSources")

	var apiURL string
	switch p.mediaServerType {
	case constants.EMBY:
		params.Add("api_key", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/emby/Items?%s", p.mediaServerAddr, params.Encode())
	case constants.JELLYFIN:
		params.Add("ApiKey", p.mediaServerAPIKey)
		apiURL = fmt.Sprintf("%s/Items?%s", p.mediaServerAddr, params.Encode())
	default:
		return "", "", fmt.Errorf("不支持的媒体服务器类型")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	bodyStr := string(body[:n])

	itemPath := gjson.Get(bodyStr, "Items.0.Path").String()
	mediaSources := gjson.Get(bodyStr, "Items.0.MediaSources").Array()

	if len(itemPath) < 5 || itemPath[len(itemPath)-5:] != ".strm" {
		return "", "", nil
	}

	strmFileType, _ := recgonizeStrmFileType(itemPath)
	if strmFileType != constants.HTTPStrm {
		return "", "", nil
	}

	if len(mediaSources) == 0 {
		return "", "", fmt.Errorf("MediaSources 为空")
	}

	content = mediaSources[0].Get("Path").String()
	return content, "", nil
}

// extractExpiresFromURL 从 URL 中提取过期时间
func (p *PrefetchService) extractExpiresFromURL(u string) time.Time {
	parsed, err := url.Parse(u)
	if err != nil {
		return time.Now().Add(30 * time.Minute)
	}

	query := parsed.Query()
	expiresParams := []string{"expires", "expire", "exp", "deadline", "sign", "token"}

	for _, param := range expiresParams {
		expStr := query.Get(param)
		if expStr == "" {
			continue
		}

		if ts, err := strconv.ParseInt(expStr, 10, 64); err == nil {
			t := time.Unix(ts, 0)
			if t.After(time.Now()) && t.Before(time.Now().Add(365*24*time.Hour)) {
				return t.Add(-5 * time.Minute)
			}
		}
	}

	return time.Now().Add(30 * time.Minute)
}

// GetCachedURL 获取缓存的预提取直链，返回空字符串表示未命中
func (p *PrefetchService) GetCachedURL(itemID string) string {
	if p == nil || p.cache == nil {
		return ""
	}

	entryBytes, err := p.cache.Get(itemID)
	if err != nil {
		return ""
	}

	// 解析 "finalURL|expiresAt|createdAt"
	parts := make([]string, 0, 3)
	current := ""
	for _, b := range entryBytes {
		if b == '|' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(b)
		}
	}
	parts = append(parts, current)

	if len(parts) < 2 {
		p.cache.Delete(itemID)
		return ""
	}

	finalURL := parts[0]
	expiresAt, _ := strconv.ParseInt(parts[1], 10, 64)

	if time.Now().Unix() > expiresAt {
		logging.Infof("预提取缓存已过期: %s", itemID)
		p.cache.Delete(itemID)
		return ""
	}

	ttl := time.Until(time.Unix(expiresAt, 0))
	logging.Infof("命中预提取缓存: %s，剩余有效时间: %v", itemID, ttl.Round(time.Second))
	return finalURL
}

var _ = (*PrefetchService)(nil)
