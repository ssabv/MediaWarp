package handler

import (
	"MediaWarp/constants"
	"MediaWarp/internal/config"
	"MediaWarp/internal/logging"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// playbackTimer 播放定时器，用于定时刷新预提取缓存
type playbackTimer struct {
	ticker     *time.Ticker
	stopChan   chan struct{}
	nextID     string // 下一集 ID
	runTimeSec int64  // 当前集时长（秒）
}

// PrefetchService 下一集直链预提取服务
type PrefetchService struct {
	httpStrmHandler   StrmHandlerFunc // 复用主 handler 的 bigcache
	mediaServerType   constants.MediaServerType
	mediaServerAddr   string
	mediaServerAPIKey string

	mu     sync.Mutex
	timers map[string]*playbackTimer

	sem chan struct{}
}

var prefetchService *PrefetchService

func GetPrefetchService() *PrefetchService {
	return prefetchService
}

func InitPrefetchService(mediaServerType constants.MediaServerType, addr string, apiKey string, httpStrmHandler StrmHandlerFunc) error {
	if !config.Prefetch.Enable {
		logging.Info("预提取服务未启用")
		return nil
	}

	maxConcurrent := config.Prefetch.MaxConcurrentPrefetch
	if maxConcurrent <= 0 {
		maxConcurrent = 3
	}

	prefetchService = &PrefetchService{
		httpStrmHandler:   httpStrmHandler,
		mediaServerType:   mediaServerType,
		mediaServerAddr:   addr,
		mediaServerAPIKey: apiKey,
		timers:            make(map[string]*playbackTimer),
		sem:               make(chan struct{}, maxConcurrent),
	}

	logging.Infof("预提取服务已启用（定时刷新模式，间隔: %.0f%% 视频时长，最大并发: %d）",
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

	runTimeTicks, seriesID, seasonID, currentIndex, err := p.queryItemRuntime(itemID)
	if err != nil {
		logging.Debugf("预提取：查询剧集信息失败: %v", err)
		return
	}
	if runTimeTicks <= 0 {
		logging.Debugf("预提取：剧集 %s 运行时为 0，跳过", itemID)
		return
	}

	runTimeSec := runTimeTicks / 10000000

	if seriesID == "" || seasonID == "" || currentIndex < 0 {
		logging.Debugf("预提取：剧集 %s 缺少 SeriesID/SeasonID/IndexNumber", itemID)
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

	go p.doPrefetch(nextItemID)
	p.stopTimer(itemID)

	refreshInterval := float64(runTimeSec) * config.Prefetch.RefreshInterval
	if refreshInterval < 30 {
		refreshInterval = 30
	}
	interval := time.Duration(refreshInterval) * time.Second

	timer := &playbackTimer{
		ticker:     time.NewTicker(interval),
		stopChan:   make(chan struct{}),
		nextID:     nextItemID,
		runTimeSec: runTimeSec,
	}

	p.mu.Lock()
	p.timers[itemID] = timer
	p.mu.Unlock()

	logging.Infof("预提取：启动定时器，每 %v 刷新一次下一集 %s 的直链（%.0f 分钟视频，每 %.0f 分钟刷新）",
		interval, nextItemID, float64(runTimeSec)/60.0, interval.Seconds()/60.0)

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

// doPrefetch 执行预提取，调用主 handler 的 httpStrmHandler 写入共享 bigcache
func (p *PrefetchService) doPrefetch(itemID string) {
	p.sem <- struct{}{}
	defer func() { <-p.sem }()

	logging.Infof("预提取：开始解析剧集 %s 的直链", itemID)

	contents, _, err := p.getStrmContent(itemID)
	if err != nil {
		logging.Warningf("预提取：获取剧集 %s 的 Strm 内容失败: %v", itemID, err)
		return
	}
	if len(contents) == 0 {
		logging.Debugf("预提取：剧集 %s 不是 HTTPStrm，跳过", itemID)
		return
	}

	for _, content := range contents {
		finalURL := p.httpStrmHandler(content, "")
		if finalURL == "" {
			logging.Warningf("预提取：解析剧集 %s 直链失败", itemID)
			continue
		}
		logging.Infof("预提取：剧集 %s 直链已缓存", itemID)
	}
}

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

func (p *PrefetchService) getStrmContent(itemID string) (contents []string, ua string, err error) {
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
		return nil, "", fmt.Errorf("不支持的媒体服务器类型")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(apiURL)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body := make([]byte, 16384)
	n, _ := resp.Body.Read(body)
	bodyStr := string(body[:n])

	itemPath := gjson.Get(bodyStr, "Items.0.Path").String()
	mediaSources := gjson.Get(bodyStr, "Items.0.MediaSources").Array()

	if len(itemPath) < 5 || itemPath[len(itemPath)-5:] != ".strm" {
		return nil, "", nil
	}

	strmFileType, _ := recgonizeStrmFileType(itemPath)
	if strmFileType != constants.HTTPStrm {
		return nil, "", nil
	}

	if len(mediaSources) == 0 {
		return nil, "", fmt.Errorf("MediaSources 为空")
	}

	for _, mediaSource := range mediaSources {
		content := mediaSource.Get("Path").String()
		if content != "" {
			contents = append(contents, content)
		}
	}
	return contents, "", nil
}
