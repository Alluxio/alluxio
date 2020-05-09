package factory

type UserData struct {
	CftConfigure UserDataConfigure `json:"cft_configure"`
}

type UserDataConfigure struct {
	AlluxioDailyBackupTime  string `json:"alluxio_daily_backup_time,omitempty"`
	AlluxioRole             string `json:"alluxio_role"`
	AlluxioUfsAddress       string `json:"alluxio_ufs_address"`
	AlluxioWorkerMemPercent string `json:"alluxio_worker_mem_percent,omitempty"` // if not given, configure 70% of the instance memory
	AlluxioWorkerSSDSize    string `json:"alluxio_worker_ssd_size,omitempty"`
	CftStack                string `json:"cf_stack"`
	StartAlluxio            bool   `json:"start_alluxio"`

	// Use interface{} instead of string to avoid double quotes ""value""
	AlluxioBackupDirectory interface{} `json:"alluxio_backup_directory,omitempty"` // only used by advanced template
	AlluxioMasterDns       interface{} `json:"alluxio_master_dns"`                 // shared by basic and advanced
	AlluxioProperties      interface{} `json:"alluxio_properties"`                 // shared by basic and advanced
	AlluxioRestoreUri      interface{} `json:"alluxio_restore_uri,omitempty"`      // only used by advanced template
}

const (
	MasterRole  = "Master"
	WorkerRole  = "Worker"

	MasterJournalDevice     = "/dev/xvdd"
	WorkerSSDDevice         = "/dev/xvdz"
)
