package middleware

import (
	"MediaWarp/internal/handler"
	"MediaWarp/internal/logging"
	"bytes"
	"io"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// PrefetchProgress 拦截播放进度上报，触发下一集直链预提取
func PrefetchProgress() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// 只拦截 POST 请求
		if ctx.Request.Method != "POST" {
			ctx.Next()
			return
		}

		// 检查是否是播放进度上报路径
		path := strings.ToLower(ctx.Request.URL.Path)
		isProgressPath := strings.Contains(path, "/sessions/playing/progress")

		if !isProgressPath {
			ctx.Next()
			return
		}

		logging.Infof("拦截播放进度上报: %s, query: %s", ctx.Request.URL.Path, ctx.Request.URL.RawQuery)

		// 优先从查询参数获取
		itemID := ctx.Query("ItemId")
		posStr := ctx.Query("PositionTicks")

		// 如果查询参数中没有，尝试从 POST body JSON 中获取
		if itemID == "" || posStr == "" {
			bodyBytes, err := io.ReadAll(ctx.Request.Body)
			if err == nil && len(bodyBytes) > 0 {
				// 恢复 body 供下游使用
				ctx.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

				bodyStr := string(bodyBytes)
				if strings.HasPrefix(strings.TrimSpace(bodyStr), "{") {
					if itemID == "" {
						itemID = gjson.Get(bodyStr, "ItemId").String()
					}
					if posStr == "" {
						posStr = gjson.Get(bodyStr, "PositionTicks").String()
						if posStr == "0" || posStr == "" {
							// PositionTicks 可能是数字类型
							pt := gjson.Get(bodyStr, "PositionTicks")
							if pt.Type == gjson.Number {
								posStr = pt.Raw
							}
						}
					}
				}
			}
		}

		if itemID == "" || posStr == "" {
			logging.Debugf("播放进度上报缺少参数: ItemId=%s, PositionTicks=%s", itemID, posStr)
			ctx.Next()
			return
		}

		positionTicks, err := strconv.ParseInt(posStr, 10, 64)
		if err != nil {
			logging.Debugf("解析 PositionTicks 失败: %v, raw=%s", err, posStr)
			ctx.Next()
			return
		}

		logging.Infof("触发预提取检查: ItemId=%s, PositionTicks=%d", itemID, positionTicks)

		// 异步处理，不阻塞主请求
		prefetchSvc := handler.GetPrefetchService()
		if prefetchSvc != nil {
			go prefetchSvc.OnPlaybackProgress(itemID, positionTicks)
		} else {
			logging.Debug("预提取服务未初始化，跳过")
		}

		ctx.Next()
	}
}
