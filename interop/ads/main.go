// Envoy-compatible ADS management server for vane's interop test.
//
// Serves one Cluster ("shop" → the -upstream address) and one
// RouteConfiguration (host shop.example.com, prefix /api) to any ADS
// client whose node id matches -node.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resource "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	server "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	types "github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	port := flag.Uint("port", 18000, "ADS gRPC port")
	nodeID := flag.String("node", "vane-envoy-e2e", "node id to serve")
	upstream := flag.String("upstream", "127.0.0.1:18081", "shop backend address")
	flag.Parse()

	ctx := context.Background()
		c := cache.NewSnapshotCache(false, cache.IDHash{}, verboseLogger{})

	host, portValue := splitHostPort(*upstream)
	shop := &cluster.Cluster{
		Name:                 "shop",
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_STATIC},
		ConnectTimeout:       durationpb.New(2 * time.Second),
		LoadAssignment: &endpoint.ClusterLoadAssignment{
			ClusterName: "shop",
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: []*endpoint.LbEndpoint{{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{Endpoint: &endpoint.Endpoint{
						Address: &core.Address{Address: &core.Address_SocketAddress{
							SocketAddress: &core.SocketAddress{
								Protocol:      core.SocketAddress_TCP,
								Address:       host,
								PortSpecifier: &core.SocketAddress_PortValue{PortValue: portValue},
							},
						}},
					}},
				}},
			}},
		},
	}

	rc := &route.RouteConfiguration{
		Name: "routes",
		VirtualHosts: []*route.VirtualHost{{
			Name:    "shop-vh",
			Domains: []string{"shop.example.com"},
			Routes: []*route.Route{{
				Match:  &route.RouteMatch{PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/api"}},
				Action: &route.Route_Route{Route: &route.RouteAction{ClusterSpecifier: &route.RouteAction_Cluster{Cluster: "shop"}}},
			}},
		}},
	}

	snap, err := cache.NewSnapshot("1", map[resource.Type][]types.Resource{
		resource.ClusterType: {shop},
		resource.RouteType:   {rc},
	})
	if err != nil {
		log.Fatalf("snapshot: %v", err)
	}
	if err := c.SetSnapshot(ctx, *nodeID, snap); err != nil {
		log.Fatalf("set snapshot: %v", err)
	}

	lis, err := net.Listen("tcp", net.JoinHostPort("", itoa(int(*port))))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := server.NewServer(ctx, c, verboseCallbacks{})
	grpcServer := grpc.NewServer()
	discovery.RegisterAggregatedDiscoveryServiceServer(grpcServer, srv)
	log.Printf("ADS serving node=%s on :%d (shop → %s)", *nodeID, *port, *upstream)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func splitHostPort(addr string) (string, uint32) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			n := uint32(0)
			for _, ch := range addr[i+1:] {
				n = n*10 + uint32(ch-'0')
			}
			return addr[:i], n
		}
	}
	return addr, 80
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

type verboseLogger struct{}

func (verboseLogger) Debugf(format string, args ...interface{}) {
	log.Printf("DEBUG "+format, args...)
}
func (verboseLogger) Infof(format string, args ...interface{}) {
	log.Printf("INFO "+format, args...)
}
func (verboseLogger) Warnf(format string, args ...interface{}) {
	log.Printf("WARN "+format, args...)
}
func (verboseLogger) Errorf(format string, args ...interface{}) {
	log.Printf("ERROR "+format, args...)
}

type verboseCallbacks struct{}

func (verboseCallbacks) OnStreamOpen(ctx context.Context, id int64, typ string) error {
	log.Printf("stream %d open %s", id, typ)
	return nil
}
func (verboseCallbacks) OnStreamClosed(id int64, node *core.Node) {}
func (verboseCallbacks) OnStreamRequest(id int64, req *discovery.DiscoveryRequest) error {
	log.Printf("stream %d request %s node=%q rn=%v", id, req.GetTypeUrl(), req.GetNode().GetId(), req.GetResourceNames())
	return nil
}
func (verboseCallbacks) OnStreamResponse(ctx context.Context, id int64, req *discovery.DiscoveryRequest, res *discovery.DiscoveryResponse) {
	log.Printf("stream %d response %s", id, req.GetTypeUrl())
}
func (verboseCallbacks) OnFetchRequest(ctx context.Context, req *discovery.DiscoveryRequest) error {
	return nil
}
func (verboseCallbacks) OnFetchResponse(req *discovery.DiscoveryRequest, resp *discovery.DiscoveryResponse) {
}
func (verboseCallbacks) OnDeltaStreamOpen(ctx context.Context, id int64, typ string) error {
	return nil
}
func (verboseCallbacks) OnDeltaStreamClosed(id int64, node *core.Node) {}
func (verboseCallbacks) OnStreamDeltaResponse(id int64, req *discovery.DeltaDiscoveryRequest, resp *discovery.DeltaDiscoveryResponse) {}
func (verboseCallbacks) OnStreamDeltaRequest(id int64, req *discovery.DeltaDiscoveryRequest) error {
	return nil
}
