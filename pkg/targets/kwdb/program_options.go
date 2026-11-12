package kwdb

type LoadingOptions struct {
	User        string
	Pass        string
	Host        string
	// Hosts is an optional CSV of host:port endpoints. When set, workers
	// round-robin across endpoints (workerNum % len(endpoints)) and Host/Port
	// are only used as fallback for bare hosts missing an explicit port.
	Hosts       string
	Port        int
	Buffer      int
	DBName      string
	Type        string
	Case        string
	Workers     int
	DoCreate    bool
	Preparesize int
	CertDir     string
	Partition   bool
}
