package usecases_postgresql

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"databasus-backend/internal/config"
	common "databasus-backend/internal/features/backups/backups/common"
	backup_encryption "databasus-backend/internal/features/backups/backups/encryption"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	pgtypes "databasus-backend/internal/features/databases/databases/postgresql"
	encryption_secrets "databasus-backend/internal/features/encryption/secrets"
	"databasus-backend/internal/features/storages"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/tools"

	"github.com/google/uuid"
)

const (
	pitrBackupTimeout         = 23 * time.Hour
	pitrShutdownCheckInterval = 1 * time.Second
	pitrCopyBufferSize        = 8 * 1024 * 1024
	pitrProgressIntervalMB    = 1.0
	pitrCompressionLevel      = 5
)

type CreatePostgresqlPitrBackupUsecase struct {
	logger           *slog.Logger
	secretKeyService *encryption_secrets.SecretKeyService
	fieldEncryptor   encryption.FieldEncryptor
}

func (uc *CreatePostgresqlPitrBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupConfig *backups_config.BackupConfig,
	db *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(
		completedMBs float64,
	),
) (*common.BackupMetadata, error) {
	uc.logger.Info(
		"Creating PostgreSQL PITR base backup via pg_basebackup",
		"databaseId",
		db.ID,
		"storageId",
		storage.ID,
	)

	pg := db.Postgresql
	if pg == nil {
		return nil, fmt.Errorf("postgresql database configuration is required for PITR backups")
	}

	decryptedPassword, err := uc.fieldEncryptor.Decrypt(db.ID, pg.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt database password: %w", err)
	}

	return uc.streamToStorage(
		ctx,
		backupID,
		backupConfig,
		uc.resolvePgBasebackupPath(
			tools.GetPostgresqlExecutable(
				pg.Version,
				"pg_basebackup",
				config.GetEnv().EnvMode,
				config.GetEnv().PostgresesInstallDir,
			),
			pg.Version,
		),
		uc.buildPgBasebackupArgs(pg),
		decryptedPassword,
		storage,
		db,
		backupProgressListener,
	)
}

func (uc *CreatePostgresqlPitrBackupUsecase) buildPgBasebackupArgs(
	pg *pgtypes.PostgresqlDatabase,
) []string {
	return []string{
		"-D", "-",
		"-Ft",
		"-Z", strconv.Itoa(pitrCompressionLevel),
		"-X", "fetch",
		"--no-password",
		"-h", pg.Host,
		"-p", strconv.Itoa(pg.Port),
		"-U", pg.Username,
	}
}

// streamToStorage streams pg_basebackup tar output directly to storage
func (uc *CreatePostgresqlPitrBackupUsecase) streamToStorage(
	parentCtx context.Context,
	backupID uuid.UUID,
	backupConfig *backups_config.BackupConfig,
	pgBin string,
	args []string,
	password string,
	storage *storages.Storage,
	db *databases.Database,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	uc.logger.Info("Streaming PostgreSQL PITR backup to storage", "pgBin", pgBin, "args", args)

	ctx, cancel := uc.createBackupContext(parentCtx)
	defer cancel()

	pgpassFile, err := uc.setupPgpassFile(db.Postgresql, password)
	if err != nil {
		return nil, err
	}
	defer func() {
		if pgpassFile != "" {
			_ = os.RemoveAll(filepath.Dir(pgpassFile))
		}
	}()

	cmd := exec.CommandContext(ctx, pgBin, args...)
	uc.logger.Info("Executing PostgreSQL PITR backup command", "command", cmd.String())

	if err := uc.setupPgEnvironment(
		cmd,
		pgpassFile,
		db.Postgresql.IsHttps,
		pgBin,
	); err != nil {
		return nil, err
	}

	pgStdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}

	pgStderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	stderrCh := make(chan []byte, 1)
	go func() {
		stderrOutput, _ := io.ReadAll(pgStderr)
		stderrCh <- stderrOutput
	}()

	storageReader, storageWriter := io.Pipe()

	finalWriter, encryptionWriter, backupMetadata, err := uc.setupBackupEncryption(
		backupID,
		backupConfig,
		storageWriter,
	)
	if err != nil {
		return nil, err
	}

	countingWriter := common.NewCountingWriter(finalWriter)

	saveErrCh := make(chan error, 1)
	go func() {
		saveErr := storage.SaveFile(ctx, uc.fieldEncryptor, uc.logger, backupID, storageReader)
		saveErrCh <- saveErr
	}()

	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", filepath.Base(pgBin), err)
	}

	copyResultCh := make(chan error, 1)
	bytesWrittenCh := make(chan int64, 1)
	go func() {
		bytesWritten, err := uc.copyWithShutdownCheck(
			ctx,
			countingWriter,
			pgStdout,
			backupProgressListener,
		)
		bytesWrittenCh <- bytesWritten
		copyResultCh <- err
	}()

	copyErr := <-copyResultCh
	bytesWritten := <-bytesWrittenCh
	waitErr := cmd.Wait()

	select {
	case <-ctx.Done():
		uc.cleanupOnCancellation(encryptionWriter, storageWriter, saveErrCh)
		return nil, uc.checkCancellationReason()
	default:
	}

	if err := uc.closeWriters(encryptionWriter, storageWriter); err != nil {
		<-saveErrCh
		return nil, err
	}

	saveErr := <-saveErrCh
	stderrOutput := <-stderrCh

	if waitErr == nil && copyErr == nil && saveErr == nil && backupProgressListener != nil {
		sizeMB := float64(bytesWritten) / (1024 * 1024)
		backupProgressListener(sizeMB)
	}

	switch {
	case waitErr != nil:
		if err := uc.checkCancellation(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("pg_basebackup failed: %v – stderr: %s", waitErr, string(stderrOutput))
	case copyErr != nil:
		if err := uc.checkCancellation(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("copy to storage: %w", copyErr)
	case saveErr != nil:
		if err := uc.checkCancellation(ctx); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("save to storage: %w", saveErr)
	}

	return &backupMetadata, nil
}

func (uc *CreatePostgresqlPitrBackupUsecase) copyWithShutdownCheck(
	ctx context.Context,
	dst io.Writer,
	src io.Reader,
	backupProgressListener func(completedMBs float64),
) (int64, error) {
	buf := make([]byte, pitrCopyBufferSize)
	var totalBytesWritten int64
	var lastReportedMB float64

	for {
		select {
		case <-ctx.Done():
			return totalBytesWritten, fmt.Errorf("copy cancelled: %w", ctx.Err())
		default:
		}

		if config.IsShouldShutdown() {
			return totalBytesWritten, fmt.Errorf("copy cancelled due to shutdown")
		}

		bytesRead, readErr := src.Read(buf)
		if bytesRead > 0 {
			writeResultCh := make(chan writeResult, 1)
			go func() {
				bytesWritten, writeErr := dst.Write(buf[0:bytesRead])
				writeResultCh <- writeResult{bytesWritten, writeErr}
			}()

			var bytesWritten int
			var writeErr error

			select {
			case <-ctx.Done():
				return totalBytesWritten, fmt.Errorf("copy cancelled during write: %w", ctx.Err())
			case result := <-writeResultCh:
				bytesWritten = result.bytesWritten
				writeErr = result.writeErr
			}

			if bytesWritten < 0 || bytesRead < bytesWritten {
				bytesWritten = 0
				if writeErr == nil {
					writeErr = fmt.Errorf("invalid write result")
				}
			}

			if writeErr != nil {
				return totalBytesWritten, writeErr
			}

			if bytesRead != bytesWritten {
				return totalBytesWritten, io.ErrShortWrite
			}

			totalBytesWritten += int64(bytesWritten)

			if backupProgressListener != nil {
				currentSizeMB := float64(totalBytesWritten) / (1024 * 1024)
				if currentSizeMB >= lastReportedMB+pitrProgressIntervalMB {
					backupProgressListener(currentSizeMB)
					lastReportedMB = currentSizeMB
				}
			}
		}

		if readErr != nil {
			if readErr != io.EOF {
				return totalBytesWritten, readErr
			}
			break
		}
	}

	return totalBytesWritten, nil
}

func (uc *CreatePostgresqlPitrBackupUsecase) createBackupContext(
	parentCtx context.Context,
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parentCtx, pitrBackupTimeout)

	go func() {
		ticker := time.NewTicker(pitrShutdownCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-parentCtx.Done():
				cancel()
				return
			case <-ticker.C:
				if config.IsShouldShutdown() {
					cancel()
					return
				}
			}
		}
	}()

	return ctx, cancel
}

func (uc *CreatePostgresqlPitrBackupUsecase) setupPgpassFile(
	pgConfig *pgtypes.PostgresqlDatabase,
	password string,
) (string, error) {
	if pgConfig == nil || password == "" {
		return "", nil
	}

	return uc.createTempPgpassFile(pgConfig, password)
}

func (uc *CreatePostgresqlPitrBackupUsecase) setupPgEnvironment(
	cmd *exec.Cmd,
	pgpassFile string,
	shouldRequireSSL bool,
	pgBin string,
) error {
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "PGPASSFILE="+pgpassFile)

	cmd.Env = append(cmd.Env,
		"PGCLIENTENCODING=UTF8",
		"PGCONNECT_TIMEOUT="+strconv.Itoa(pgConnectTimeout),
		"LC_ALL=C.UTF-8",
		"LANG=C.UTF-8",
	)

	if shouldRequireSSL {
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")
		uc.logger.Info("Using required SSL mode for PITR", "configuredHttps", shouldRequireSSL)
	} else {
		cmd.Env = append(cmd.Env, "PGSSLMODE=prefer")
		uc.logger.Info("Using preferred SSL mode for PITR", "configuredHttps", shouldRequireSSL)
	}

	cmd.Env = append(cmd.Env,
		"PGSSLCERT=",
		"PGSSLKEY=",
		"PGSSLROOTCERT=",
		"PGSSLCRL=",
	)

	if _, err := exec.LookPath(pgBin); err != nil {
		return fmt.Errorf("PostgreSQL executable not found or not accessible: %s - %w", pgBin, err)
	}

	return nil
}

func (uc *CreatePostgresqlPitrBackupUsecase) setupBackupEncryption(
	backupID uuid.UUID,
	backupConfig *backups_config.BackupConfig,
	storageWriter io.WriteCloser,
) (io.Writer, *backup_encryption.EncryptionWriter, common.BackupMetadata, error) {
	metadata := common.BackupMetadata{}

	if backupConfig.Encryption != backups_config.BackupEncryptionEncrypted {
		metadata.Encryption = backups_config.BackupEncryptionNone
		uc.logger.Info("Encryption disabled for PITR backup", "backupId", backupID)
		return storageWriter, nil, metadata, nil
	}

	salt, err := backup_encryption.GenerateSalt()
	if err != nil {
		return nil, nil, metadata, fmt.Errorf("failed to generate salt: %w", err)
	}

	nonce, err := backup_encryption.GenerateNonce()
	if err != nil {
		return nil, nil, metadata, fmt.Errorf("failed to generate nonce: %w", err)
	}

	masterKey, err := uc.secretKeyService.GetSecretKey()
	if err != nil {
		return nil, nil, metadata, fmt.Errorf("failed to get master key: %w", err)
	}

	encWriter, err := backup_encryption.NewEncryptionWriter(
		storageWriter,
		masterKey,
		backupID,
		salt,
		nonce,
	)
	if err != nil {
		return nil, nil, metadata, fmt.Errorf("failed to create encrypting writer: %w", err)
	}

	saltBase64 := base64.StdEncoding.EncodeToString(salt)
	nonceBase64 := base64.StdEncoding.EncodeToString(nonce)
	metadata.EncryptionSalt = &saltBase64
	metadata.EncryptionIV = &nonceBase64
	metadata.Encryption = backups_config.BackupEncryptionEncrypted

	uc.logger.Info("Encryption enabled for PITR backup", "backupId", backupID)
	return encWriter, encWriter, metadata, nil
}

func (uc *CreatePostgresqlPitrBackupUsecase) cleanupOnCancellation(
	encryptionWriter *backup_encryption.EncryptionWriter,
	storageWriter io.WriteCloser,
	saveErrCh chan error,
) {
	if encryptionWriter != nil {
		go func() {
			if closeErr := encryptionWriter.Close(); closeErr != nil {
				uc.logger.Error(
					"Failed to close encrypting writer during PITR cancellation",
					"error",
					closeErr,
				)
			}
		}()
	}

	if err := storageWriter.Close(); err != nil {
		uc.logger.Error("Failed to close pipe writer during PITR cancellation", "error", err)
	}

	<-saveErrCh
}

func (uc *CreatePostgresqlPitrBackupUsecase) closeWriters(
	encryptionWriter *backup_encryption.EncryptionWriter,
	storageWriter io.WriteCloser,
) error {
	encryptionCloseErrCh := make(chan error, 1)
	if encryptionWriter != nil {
		go func() {
			closeErr := encryptionWriter.Close()
			if closeErr != nil {
				uc.logger.Error("Failed to close encrypting writer", "error", closeErr)
			}
			encryptionCloseErrCh <- closeErr
		}()
	} else {
		encryptionCloseErrCh <- nil
	}

	encryptionCloseErr := <-encryptionCloseErrCh
	if encryptionCloseErr != nil {
		if err := storageWriter.Close(); err != nil {
			uc.logger.Error("Failed to close pipe writer after encryption error", "error", err)
		}
		return fmt.Errorf("failed to close encryption writer: %w", encryptionCloseErr)
	}

	if err := storageWriter.Close(); err != nil {
		uc.logger.Error("Failed to close pipe writer", "error", err)
		return err
	}

	return nil
}

func (uc *CreatePostgresqlPitrBackupUsecase) checkCancellation(ctx context.Context) error {
	select {
	case <-ctx.Done():
		if config.IsShouldShutdown() {
			return fmt.Errorf("backup cancelled due to shutdown")
		}
		return fmt.Errorf("backup cancelled")
	default:
		return nil
	}
}

func (uc *CreatePostgresqlPitrBackupUsecase) checkCancellationReason() error {
	if config.IsShouldShutdown() {
		return fmt.Errorf("backup cancelled due to shutdown")
	}
	return fmt.Errorf("backup cancelled")
}

func (uc *CreatePostgresqlPitrBackupUsecase) resolvePgBasebackupPath(
	pgBin string,
	version tools.PostgresqlVersion,
) string {
	if _, err := os.Stat(pgBin); err == nil {
		return pgBin
	}

	fallbackBin, err := exec.LookPath("pg_basebackup")
	if err == nil {
		uc.logger.Warn(
			"pg_basebackup not found for requested PostgreSQL version, falling back to PATH",
			"version",
			version,
			"fallback",
			fallbackBin,
			"requestedPath",
			pgBin,
		)
		return fallbackBin
	}

	uc.logger.Error(
		"pg_basebackup not found for requested PostgreSQL version",
		"version",
		version,
		"requestedPath",
		pgBin,
	)

	return pgBin
}

func (uc *CreatePostgresqlPitrBackupUsecase) createTempPgpassFile(
	pgConfig *pgtypes.PostgresqlDatabase,
	password string,
) (string, error) {
	escapedHost := tools.EscapePgpassField(pgConfig.Host)
	escapedUsername := tools.EscapePgpassField(pgConfig.Username)
	escapedPassword := tools.EscapePgpassField(password)

	pgpassContent := fmt.Sprintf("%s:%d:*:%s:%s",
		escapedHost,
		pgConfig.Port,
		escapedUsername,
		escapedPassword,
	)

	tempFolder := config.GetEnv().TempFolder
	if err := os.MkdirAll(tempFolder, 0700); err != nil {
		return "", fmt.Errorf("failed to ensure temp folder exists: %w", err)
	}
	if err := os.Chmod(tempFolder, 0700); err != nil {
		return "", fmt.Errorf("failed to set temp folder permissions: %w", err)
	}

	tempDir, err := os.MkdirTemp(tempFolder, "pgpass_"+uuid.New().String())
	if err != nil {
		return "", fmt.Errorf("failed to create temporary directory: %w", err)
	}

	if err := os.Chmod(tempDir, 0700); err != nil {
		_ = os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to set temporary directory permissions: %w", err)
	}

	pgpassFile := filepath.Join(tempDir, ".pgpass")
	err = os.WriteFile(pgpassFile, []byte(pgpassContent), 0600)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to write temporary .pgpass file: %w", err)
	}

	return pgpassFile, nil
}
