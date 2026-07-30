package middleware

import (
	"MediaWarp/internal/handler"
	"MediaWarp/internal/logging"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
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

		logging.Debug("拦截播放进度上报: ", ctx.Request.URL.Path)

		// 从查询参数获取 ItemId 和 PositionTicks
		itemID := ctx.Query("ItemId")
		if itemID == "" {
			itemID = ctx.Query("itemId")
		}
		if itemID == "" {
			itemID = ctx.Query("itemid")
		}

		posStr := ctx.Query("PositionTicks")
		if posStr == "" {
			posStr = ctx.Query("positionTicks")
		}
		if posStr == "" {
			posStr = ctx.Query("positionticks")
		}

		if itemID != "" && posStr != "" {
			positionTicks, err := strconv.ParseInt(posStr, 10, 64)
			if err != nil {
				logging.Debugf("解析 PositionTicks 失败: %v", err)
			} else {
				// 异步处理，不阻塞主请求
				prefetchSvc := handler.GetPrefetchService()
				if prefetchSvc != nil {
					go prefetchSvc.OnPlaybackProgress(itemID, positionTicks)
				}
			}
		}

		ctx.Next()
	}
}
