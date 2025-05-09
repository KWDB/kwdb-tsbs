package kwdb

type LoadingOptions struct {
	User        string
	Pass        string
	Host        string
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
