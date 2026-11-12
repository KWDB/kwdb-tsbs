package commonpool

import (
	"fmt"
	"strconv"
	"strings"
)

// Endpoint is one SQL listen address for KaiwuDB / kwdb.
type Endpoint struct {
	Host string
	Port int
}

// ResolveEndpoints builds the endpoint list from an optional hosts CSV and a
// fallback single host/port. hostsCSV format: "h1:26257,h2:26257". A bare host
// without ":port" uses defaultPort. When hostsCSV is empty, host+defaultPort
// are used (legacy single-endpoint behavior).
func ResolveEndpoints(hostsCSV, host string, defaultPort int) ([]Endpoint, error) {
	hostsCSV = strings.TrimSpace(hostsCSV)
	if hostsCSV == "" {
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, fmt.Errorf("host or hosts must be set")
		}
		if defaultPort <= 0 {
			return nil, fmt.Errorf("invalid port %d", defaultPort)
		}
		return []Endpoint{{Host: host, Port: defaultPort}}, nil
	}

	parts := strings.Split(hostsCSV, ",")
	endpoints := make([]Endpoint, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		ep, err := parseEndpoint(part, defaultPort)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, ep)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("hosts is empty after parsing")
	}
	return endpoints, nil
}

func parseEndpoint(spec string, defaultPort int) (Endpoint, error) {
	// IPv4 host:port or bare host. IPv6 bracket form [addr]:port is also accepted.
	if strings.HasPrefix(spec, "[") {
		closeIdx := strings.IndexByte(spec, ']')
		if closeIdx < 0 {
			return Endpoint{}, fmt.Errorf("invalid endpoint %q: missing ']'", spec)
		}
		host := spec[1:closeIdx]
		rest := spec[closeIdx+1:]
		if rest == "" {
			if defaultPort <= 0 {
				return Endpoint{}, fmt.Errorf("invalid port %d for endpoint %q", defaultPort, spec)
			}
			return Endpoint{Host: host, Port: defaultPort}, nil
		}
		if !strings.HasPrefix(rest, ":") {
			return Endpoint{}, fmt.Errorf("invalid endpoint %q", spec)
		}
		port, err := strconv.Atoi(rest[1:])
		if err != nil || port <= 0 {
			return Endpoint{}, fmt.Errorf("invalid port in endpoint %q", spec)
		}
		return Endpoint{Host: host, Port: port}, nil
	}

	host, portStr, ok := strings.Cut(spec, ":")
	if !ok {
		if defaultPort <= 0 {
			return Endpoint{}, fmt.Errorf("invalid port %d for endpoint %q", defaultPort, spec)
		}
		if host == "" {
			return Endpoint{}, fmt.Errorf("empty host in endpoint %q", spec)
		}
		return Endpoint{Host: host, Port: defaultPort}, nil
	}
	if host == "" {
		return Endpoint{}, fmt.Errorf("empty host in endpoint %q", spec)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 {
		return Endpoint{}, fmt.Errorf("invalid port in endpoint %q", spec)
	}
	return Endpoint{Host: host, Port: port}, nil
}

// PickEndpoint selects endpoints[workerNum % len(endpoints)].
func PickEndpoint(endpoints []Endpoint, workerNum int) (Endpoint, error) {
	if len(endpoints) == 0 {
		return Endpoint{}, fmt.Errorf("no endpoints")
	}
	if workerNum < 0 {
		workerNum = -workerNum
	}
	return endpoints[workerNum%len(endpoints)], nil
}
