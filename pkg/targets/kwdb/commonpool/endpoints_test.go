package commonpool

import (
	"testing"
)

func TestResolveEndpointsSingle(t *testing.T) {
	eps, err := ResolveEndpoints("", "127.0.0.1", 26257)
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 1 || eps[0].Host != "127.0.0.1" || eps[0].Port != 26257 {
		t.Fatalf("unexpected endpoints: %+v", eps)
	}
}

func TestResolveEndpointsCSV(t *testing.T) {
	eps, err := ResolveEndpoints("a:1, b:2, c", "ignored", 9)
	if err != nil {
		t.Fatal(err)
	}
	want := []Endpoint{{"a", 1}, {"b", 2}, {"c", 9}}
	if len(eps) != len(want) {
		t.Fatalf("got %d endpoints, want %d: %+v", len(eps), len(want), eps)
	}
	for i := range want {
		if eps[i] != want[i] {
			t.Fatalf("endpoint[%d]=%+v, want %+v", i, eps[i], want[i])
		}
	}
}

func TestResolveEndpointsIPv6(t *testing.T) {
	eps, err := ResolveEndpoints("[::1]:27257", "", 26257)
	if err != nil {
		t.Fatal(err)
	}
	if len(eps) != 1 || eps[0].Host != "::1" || eps[0].Port != 27257 {
		t.Fatalf("unexpected endpoints: %+v", eps)
	}
}

func TestResolveEndpointsErrors(t *testing.T) {
	if _, err := ResolveEndpoints("", "", 26257); err == nil {
		t.Fatal("expected error for empty host/hosts")
	}
	if _, err := ResolveEndpoints("bad:port", "", 1); err == nil {
		t.Fatal("expected error for bad port")
	}
	if _, err := ResolveEndpoints(",,", "", 1); err == nil {
		t.Fatal("expected error for empty CSV")
	}
}

func TestPickEndpoint(t *testing.T) {
	eps := []Endpoint{{"a", 1}, {"b", 2}, {"c", 3}}
	for worker, wantIdx := range []int{0, 1, 2, 0, 1} {
		got, err := PickEndpoint(eps, worker)
		if err != nil {
			t.Fatal(err)
		}
		if got != eps[wantIdx] {
			t.Fatalf("worker %d: got %+v, want %+v", worker, got, eps[wantIdx])
		}
	}
	if _, err := PickEndpoint(nil, 0); err == nil {
		t.Fatal("expected error for empty endpoints")
	}
}
