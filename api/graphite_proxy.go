package api

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/grafana/metrictank/stats"
)

var proxyStats graphiteProxyStats

func init() {
	proxyStats = graphiteProxyStats{
		funcMiss: make(map[string]*stats.Counter32),
	}

}

type graphiteProxyStats struct {
	sync.Mutex
	funcMiss map[string]*stats.Counter32
}

func (s *graphiteProxyStats) Miss(fun string) {
	s.Lock()
	counter, ok := s.funcMiss[fun]
	if !ok {
		counter = stats.NewCounter32(fmt.Sprintf("api.request.render.proxy-due-to.%s", fun))
		s.funcMiss[fun] = counter
	}
	s.Unlock()
	counter.Inc()
}

func NewGraphiteProxy(u *url.URL) *httputil.ReverseProxy {
	graphiteProxy := httputil.NewSingleHostReverseProxy(u)
	// remove these headers from upstream. we will set our own correct ones
	graphiteProxy.ModifyResponse = func(resp *http.Response) error {
		// if kept, would be duplicated. and duplicated headers are illegal)
		resp.Header.Del("access-control-allow-credentials")
		resp.Header.Del("access-control-allow-origin")
		// if kept, would errorously stick around and be invalid because we gzip responses
		resp.Header.Del("content-length")
		return nil
	}
	return graphiteProxy
}
