package middleware

import (
	"MediaWarp/internal/handler"
	"MediaWarp/internal/logging"
	"bytes"
	"io"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// PrefetchProgress 拦截播放开始/停止事件，触发下一集直链预提取
func PrefetchProgress() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if ctx.Request.Method != "POST" {
			ctx.Next()
			return
		}

		path := strings.ToLower(ctx.Request.URL.Path)

		// 拦截播放开始: POST /Sessions/Playing
		isPlaybackStart := strings.HasSuffix(path, "/sessions/playing")

		// 拦截播放停止: POST /Sessions/Playing/Stopped
		isPlaybackStop := strings.Contains(path, "/sessions/playing/stopped")

		if !isPlaybackStart && !isPlaybackStop {
			ctx.Next()
			return
		}

		// 读取 body 获取 ItemId
		itemID := ctx.Query("ItemId")
		if itemID == "" {
			bodyBytes, err := io.ReadAll(ctx.Request.Body)
			if err == nil && len(bodyBytes) > 0 {
				ctx.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				bodyStr := string(bodyBytes)
				if strings.HasPrefix(strings.TrimSpace(bodyStr), "{") {
					itemID = gjson.Get(bodyStr, "ItemId").String()
				}
			}
		}

		if itemID == "" {
			ctx.Next()
			return
		}

		prefetchSvc := handler.GetPrefetchService()
		if prefetchSvc == nil {
			ctx.Next()
			return
		}

		if isPlaybackStart {
			logging.Infof("检测到播放开始: ItemId=%s", itemID)
			go prefetchSvc.OnPlaybackStart(itemID)
		} else if isPlaybackStop {
			logging.Infof("检测到播放停止: ItemId=%s", itemID)
			go prefetchSvc.OnPlaybackStop(itemID)
		}

		ctx.Next()
	}
}
