package handler

import (
	"MediaWarp/internal/config"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// 启用 HTTPStrm 最终 URL 与两级缓存
func setupStrmTestConfig() {
	config.HTTPStrm.FinalURL = true
	config.Cache.Enable = true
	config.Cache.HTTPStrmTTL = time.Minute
	config.Prefetch.Enable = true
	config.Prefetch.PrefetchCacheTTL = time.Minute
}

// 回归测试：解析最终 URL 失败（上游不可用）时不得写入缓存，且应回退返回原始 URL。
//
// 旧实现会在失败时把空字符串写进缓存，之后每次请求都命中这个空值并不断续期 TTL，
// 导致该路径后续播放全部失败（302 跳转到空地址），只能重启进程恢复。
func TestHTTPStrmHandlerFallbackOnFailure(t *testing.T) {
	setupStrmTestConfig()

	// 占一个端口后立即关闭，模拟 123pan-strm 接口不可用（连接被拒绝/超时）
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听测试端口失败：%v", err)
	}
	origin := "http://" + listener.Addr().String() + "/play/abc"
	listener.Close()

	handler, err := getHTTPStrmHandler()
	if err != nil {
		t.Fatalf("创建 HTTPStrm 处理器失败：%v", err)
	}

	if got := handler(origin, "test-ua"); got != origin {
		t.Fatalf("解析失败时应回退原始 URL，实际得到：%q", got)
	}

	// 第二次请求：若失败结果（空值）被写入缓存，这里会返回空串
	if got := handler(origin, "test-ua"); got != origin {
		t.Fatalf("解析失败的结果被写入了缓存，第二次请求返回：%q", got)
	}
}

// 回归测试：上游返回 5xx 时同样不能把该地址当作最终直链缓存，
// 上游恢复后应能重新解析出真正的直链
func TestHTTPStrmHandlerRetriesAfterUpstreamError(t *testing.T) {
	setupStrmTestConfig()

	var upstreamBroken atomic.Bool
	upstreamBroken.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstreamBroken.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/final" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	}))
	defer srv.Close()

	handler, err := getHTTPStrmHandler()
	if err != nil {
		t.Fatalf("创建 HTTPStrm 处理器失败：%v", err)
	}

	origin := srv.URL + "/play/abc"

	if got := handler(origin, "test-ua"); got != origin {
		t.Fatalf("上游异常时应回退原始 URL，实际得到：%q", got)
	}

	upstreamBroken.Store(false) // 上游恢复
	want := srv.URL + "/final"
	if got := handler(origin, "test-ua"); got != want {
		t.Fatalf("上游恢复后应重新解析出直链，期望 %q，实际 %q", want, got)
	}
}

// 回归测试：缓存里已存在的空值（历史版本遗留的污染条目）应视为未命中并重新解析
func TestHTTPStrmHandlerIgnoresEmptyCacheEntry(t *testing.T) {
	setupStrmTestConfig()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/final" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	}))
	defer srv.Close()

	handler, err := getHTTPStrmHandler()
	if err != nil {
		t.Fatalf("创建 HTTPStrm 处理器失败：%v", err)
	}

	if prefetchCache == nil {
		t.Fatal("预提取缓存未初始化")
	}

	origin := srv.URL + "/play/abc"

	// 构造历史版本留下的空值缓存条目
	if err := prefetchCache.Set(origin, []byte("")); err != nil {
		t.Fatalf("写入测试缓存失败：%v", err)
	}

	want := srv.URL + "/final"
	if got := handler(origin, "test-ua"); got != want {
		t.Fatalf("空值缓存应被视为未命中并重新解析，期望 %q，实际 %q", want, got)
	}
}

// 正常解析结果仍应被缓存，避免每次播放都重新解析
func TestHTTPStrmHandlerCachesSuccessfulURL(t *testing.T) {
	setupStrmTestConfig()

	var upstreamBroken atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstreamBroken.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/final" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	}))
	defer srv.Close()

	handler, err := getHTTPStrmHandler()
	if err != nil {
		t.Fatalf("创建 HTTPStrm 处理器失败：%v", err)
	}

	origin := srv.URL + "/play/abc"
	want := srv.URL + "/final"

	if got := handler(origin, "test-ua"); got != want {
		t.Fatalf("解析直链失败，期望 %q，实际 %q", want, got)
	}

	upstreamBroken.Store(true) // 上游故障后仍应命中缓存
	if got := handler(origin, "test-ua"); got != want {
		t.Fatalf("成功结果未被缓存，期望 %q，实际 %q", want, got)
	}
}
