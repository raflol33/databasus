package backuping

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/backups/backups/usecases"
	backups_wal "databasus-backend/internal/features/backups/backups/wal"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"
	tasks_cancellation "databasus-backend/internal/features/tasks/cancellation"
	workspaces_services "databasus-backend/internal/features/workspaces/services"
	cache_utils "databasus-backend/internal/util/cache"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var backupRepository = &backups_core.BackupRepository{}

var taskCancelManager = tasks_cancellation.GetTaskCancelManager()

var walReceiverService = &WalReceiverService{
	storages.GetStorageService(),
	backups_wal.GetBackupWalService(),
	encryption.GetFieldEncryptor(),
	logger.GetLogger(),
	sync.Mutex{},
	map[uuid.UUID]context.CancelFunc{},
}

var backupCleaner = &BackupCleaner{
	backupRepository,
	storages.GetStorageService(),
	backups_config.GetBackupConfigService(),
	encryption.GetFieldEncryptor(),
	logger.GetLogger(),
	[]backups_core.BackupRemoveListener{
		backups_wal.GetBackupWalService(),
		walReceiverService,
	},
	sync.Once{},
	atomic.Bool{},
}

var backupNodesRegistry = &BackupNodesRegistry{
	cache_utils.GetValkeyClient(),
	logger.GetLogger(),
	cache_utils.DefaultCacheTimeout,
	cache_utils.NewPubSubManager(),
	cache_utils.NewPubSubManager(),
	sync.Once{},
	atomic.Bool{},
}

func getNodeID() uuid.UUID {
	return uuid.New()
}

var backuperNode = &BackuperNode{
	databases.GetDatabaseService(),
	encryption.GetFieldEncryptor(),
	workspaces_services.GetWorkspaceService(),
	backupRepository,
	backups_config.GetBackupConfigService(),
	storages.GetStorageService(),
	notifiers.GetNotifierService(),
	taskCancelManager,
	backupNodesRegistry,
	walReceiverService,
	logger.GetLogger(),
	usecases.GetCreateBackupUsecase(),
	getNodeID(),
	time.Time{},
	sync.Once{},
	atomic.Bool{},
}

var backupsScheduler = &BackupsScheduler{
	backupRepository,
	backups_config.GetBackupConfigService(),
	taskCancelManager,
	backupNodesRegistry,
	time.Now().UTC(),
	logger.GetLogger(),
	make(map[uuid.UUID]BackupToNodeRelation),
	backuperNode,
	sync.Once{},
	atomic.Bool{},
}

func GetBackupsScheduler() *BackupsScheduler {
	return backupsScheduler
}

func GetBackuperNode() *BackuperNode {
	return backuperNode
}

func GetBackupNodesRegistry() *BackupNodesRegistry {
	return backupNodesRegistry
}

func GetBackupCleaner() *BackupCleaner {
	return backupCleaner
}
