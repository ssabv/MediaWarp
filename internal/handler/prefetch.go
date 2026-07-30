package handler

import (
	"MediaWarp/constants"
	"MediaWarp/internal/config"
	"MediaWarp/internal/logging"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/tidwall/gjson"
)

// PrefetchService 下一集直链预提取服务
type PrefetchService struct {
	cache             *bigcache.BigCache
	httpStrmHandler   StrmHandlerFunc
	mediaServerType   constants.MediaServerType
	mediaServerAddr   string
	mediaServerAPIKey string

	// 记录每集已触发的最高播放进度
	triggeredProgress sync.Map // map[itemID]float64

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
			nil,
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
		sem:               make(chan struct{}, maxConcurrent),
	}

	logging.Infof("预提取服务已启用（阈值: %.0f%%, 最大并发: %d）",
		config.Prefetch.ProgressThreshold*100,
		maxConcurrent,
	)
	return nil
}

// OnPlaybackProgress 处理播放进度事件
//
// itemID: 当前正在播放的媒体项 ID
// positionTicks: 当前播放位置（100 纳秒单位，即 1 tick = 100ns）
func (p *PrefetchService) OnPlaybackProgress(itemID string, positionTicks int64) {
	if !config.Prefetch.Enable {
		return
	}

	// 获取剧集的运行时信息
	runTimeTicks, seriesID, seasonID, currentIndex, err := p.queryItemRuntime(itemID)
	if err != nil {
		logging.Debugf("预提取：查询剧集信息失败: %v", err)
		return
	}

	if runTimeTicks <= 0 {
		logging.Debugf("预提取：剧集 %s 运行时为 0，跳过", itemID)
		return
	}

	// 计算播放进度
	progress := float64(positionTicks) / float64(runTimeTicks)
	remainingSeconds := float64(runTimeTicks-positionTicks) / 10000000.0 // ticks → 秒

	logging.Debugf("预提取：剧集 %s 进度 %.2f%%, 剩余 %.0f 秒", itemID, progress*100, remainingSeconds)

	// 检查是否达到触发条件
	triggered := false
	if progress >= config.Prefetch.ProgressThreshold {
		triggered = true
	}
	if config.Prefetch.MinRemainingSeconds > 0 && remainingSeconds <= config.Prefetch.MinRemainingSeconds {
		triggered = true
	}
	if !triggered {
		return
	}

	// 防止重复触发：每集只触发一次，记录最高进度
	if config.Prefetch.TriggerOncePerEpisode {
		previousTriggeredAt, _ := p.triggeredProgress.Load(itemID)
		var prevProgress float64
		if previousTriggeredAt != nil {
			prevProgress = previousTriggeredAt.(float64)
		}

		// 快进场景：当前进度超过上次触发时的进度，才再次触发
		if progress <= prevProgress {
			logging.Debugf("预提取：剧集 %s 已在进度 %.2f%% 触发过，跳过（当前 %.2f%%）",
				itemID, prevProgress*100, progress*100)
			return
		}

		p.triggeredProgress.Store(itemID, progress)
	}

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

	// 异步预取
	go p.prefetchEpisode(nextItemID)
}

// queryItemRuntime 查询剧集的运行时和剧集信息
func (p *PrefetchService) queryItemRuntime(itemID string) (runTimeTicks int64, seriesID string, seasonID string, indexNumber int64, err error) {
	params := url.Values{}
	params.Add("Ids", itemID)
	params.Add("Limit", "1")
	params.Add("Fields", "RunTimeTicks,SeriesId,SeasonId,IndexNumber,Path,MediaSources")

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

	// 遍历 Items 找到 IndexNumber == nextIndex 的剧集
	items := gjson.Get(bodyStr, "Items")
	for _, item := range items.Array() {
		if item.Get("IndexNumber").Int() == nextIndex {
			return item.Get("Id").String(), nil
		}
	}

	return "", nil
}

// prefetchEpisode 预提取剧集的直链
func (p *PrefetchService) prefetchEpisode(itemID string) {
	// 并发控制
	p.sem <- struct{}{}
	defer func() { <-p.sem }()

	// 检查是否已有缓存
	if p.cache != nil {
		if _, err := p.cache.Get(itemID); err == nil {
			logging.Debugf("预提取：剧集 %s 已有缓存，跳过", itemID)
			return
		}
	}

	logging.Infof("预提取：开始解析剧集 %s 的直链", itemID)

	// 查询剧集的 Path 和 MediaSources
	runTimeTicks, _, _, _, err := p.queryItemRuntime(itemID)
	if err != nil {
		logging.Warningf("预提取：查询剧集 %s 信息失败: %v", itemID, err)
		return
	}

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
	_ = runTimeTicks // 用于后续缓存过期时间计算
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

// getStrmContent 获取剧集的 Strm 文件内容和识别类型
func (p *PrefetchService) getStrmContent(itemID string) (content string, ua string, err error) {
	params := url.Values{}
	params.Add("Ids", itemID)
	params.Add("Limit", "1")
	// 用 MediaSources 字段获取 Protocol 和 Path 信息
	fields := "Path,MediaSources"
	switch p.mediaServerType {
	case constants.EMBY:
		fields = "Path,MediaSources"
	case constants.JELLYFIN:
		fields = "Path,MediaSources"
	}
	params.Add("Fields", fields)

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

	// 检查 Path 是否以 .strm 结尾
	if len(itemPath) < 5 || itemPath[len(itemPath)-5:] != ".strm" {
		return "", "", nil // 不是 Strm 文件
	}

	// 检查 Strm 类型和获取内容
	strmFileType, _ := recgonizeStrmFileType(itemPath)
	if strmFileType != constants.HTTPStrm {
		return "", "", nil // 不是 HTTPStrm，暂不处理
	}

	// 获取 MediaSource.Path（即 HTTPStrm 中的 HTTP URL）
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

		// 尝试解析为 Unix 时间戳（秒）
		if ts, err := strconv.ParseInt(expStr, 10, 64); err == nil {
			t := time.Unix(ts, 0)
			if t.After(time.Now()) && t.Before(time.Now().Add(365*24*time.Hour)) {
				return t.Add(-5 * time.Minute) // 提前 5 分钟失效
			}
		}
	}

	// 没有找到过期参数，默认 30 分钟
	return time.Now().Add(30 * time.Minute)
}

// GetCachedURL 获取缓存的预提取直链
// 返回空字符串表示未命中缓存
func (p *PrefetchService) GetCachedURL(itemID string) string {
	if p == nil || p.cache == nil {
		return ""
	}

	entryBytes, err := p.cache.Get(itemID)
	if err != nil {
		return ""
	}

	// 解析缓存条目: "finalURL|expiresAt|createdAt"
	parts := make([]string, 0)
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

	// 检查是否已过期
	if time.Now().Unix() > expiresAt {
		logging.Infof("预提取缓存已过期: %s，重新获取", itemID)
		p.cache.Delete(itemID)
		return ""
	}

	ttl := time.Until(time.Unix(expiresAt, 0))
	logging.Infof("命中预提取缓存: %s，剩余有效时间: %v", itemID, ttl.Round(time.Second))
	return finalURL
}

// 确保类型兼容
var _ = (*PrefetchService)(nil)
