package kwdb

import "testing"

func TestStripCPURowEnvelope(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"(1,2,'host_1')", "1,2,'host_1'"},
		{"  (1,2,'host_1')  ", "1,2,'host_1'"},
		// truncated: missing closing paren — must not eat trailing quote
		{"(1,2,'host_244'", "1,2,'host_244'"},
		{"1,2,'host_1'", "1,2,'host_1'"},
	}
	for _, tc := range cases {
		if got := stripCPURowEnvelope(tc.in); got != tc.want {
			t.Fatalf("stripCPURowEnvelope(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestExtractHostnameField(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"'host_244'", "host_244"},
		{" 'host_244' ", "host_244"},
		{"'host_244", "host_244"}, // truncated closing quote
		{"host_244", "host_244"},
	}
	for _, tc := range cases {
		if got := extractHostnameField(tc.in); got != tc.want {
			t.Fatalf("extractHostnameField(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseCPURowTruncatedEnvelope(t *testing.T) {
	p := &prepareProcessor{}
	buf := newFixedArgList(12)
	buf.Init()
	// Missing trailing ')': old s[1:len-1] would yield hostname "'host_244" and panic.
	s := stripCPURowEnvelope("(1577836800000,76,85,57,93,9,61,56,7,63,99,'host_244'")
	p.parseCPURowIntoBuffer(s, buf)
	if buf.Length() != 12 {
		t.Fatalf("len=%d", buf.Length())
	}
	if got := string(buf.args[11]); got != "host_244" {
		t.Fatalf("hostname=%q", got)
	}
}
