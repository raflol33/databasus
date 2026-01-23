package backups

import (
	"context"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_download "databasus-backend/internal/features/backups/backups/download"
	"databasus-backend/internal/features/databases"
	users_middleware "databasus-backend/internal/features/users/middleware"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type BackupController struct {
	backupService *BackupService
}

func (c *BackupController) RegisterRoutes(router *gin.RouterGroup) {
	router.GET("/backups", c.GetBackups)
	router.POST("/backups", c.MakeBackup)
	router.POST("/backups/:id/download-token", c.GenerateDownloadToken)
	router.DELETE("/backups/:id", c.DeleteBackup)
	router.POST("/backups/:id/cancel", c.CancelBackup)
}

// RegisterPublicRoutes registers routes that don't require Bearer authentication
// (they have their own authentication mechanisms like download tokens)
func (c *BackupController) RegisterPublicRoutes(router *gin.RouterGroup) {
	router.GET("/backups/:id/file", c.GetFile)
}

// GetBackups
// @Summary Get backups for a database
// @Description Get paginated backups for the specified database
// @Tags backups
// @Produce json
// @Param database_id query string true "Database ID"
// @Param limit query int false "Number of items per page" default(10)
// @Param offset query int false "Offset for pagination" default(0)
// @Success 200 {object} GetBackupsResponse
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backups [get]
func (c *BackupController) GetBackups(ctx *gin.Context) {
	user, ok := users_middleware.GetUserFromContext(ctx)
	if !ok {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "User not authenticated"})
		return
	}

	var request GetBackupsRequest
	if err := ctx.ShouldBindQuery(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	databaseID, err := uuid.Parse(request.DatabaseID)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid database_id"})
		return
	}

	response, err := c.backupService.GetBackups(user, databaseID, request.Limit, request.Offset)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, response)
}

// MakeBackup
// @Summary Create a backup
// @Description Create a new backup for the specified database
// @Tags backups
// @Accept json
// @Produce json
// @Param request body MakeBackupRequest true "Backup creation data"
// @Success 200 {object} map[string]string
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backups [post]
func (c *BackupController) MakeBackup(ctx *gin.Context) {
	user, ok := users_middleware.GetUserFromContext(ctx)
	if !ok {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "User not authenticated"})
		return
	}

	var request MakeBackupRequest
	if err := ctx.ShouldBindJSON(&request); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if request.BackupType == "" {
		request.BackupType = backups_core.BackupTypeLogical
	}

	if request.BackupType != backups_core.BackupTypeLogical &&
		request.BackupType != backups_core.BackupTypePITR {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup_type"})
		return
	}

	if err := c.backupService.MakeBackupWithAuth(user, request.DatabaseID, request.BackupType); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "backup started successfully"})
}

// DeleteBackup
// @Summary Delete a backup
// @Description Delete an existing backup
// @Tags backups
// @Param id path string true "Backup ID"
// @Success 204
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backups/{id} [delete]
func (c *BackupController) DeleteBackup(ctx *gin.Context) {
	user, ok := users_middleware.GetUserFromContext(ctx)
	if !ok {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "User not authenticated"})
		return
	}

	id, err := uuid.Parse(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup ID"})
		return
	}

	if err := c.backupService.DeleteBackup(user, id); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.Status(http.StatusNoContent)
}

// CancelBackup
// @Summary Cancel an in-progress backup
// @Description Cancel a backup that is currently in progress
// @Tags backups
// @Param id path string true "Backup ID"
// @Success 204
// @Failure 400
// @Failure 401
// @Failure 500
// @Router /backups/{id}/cancel [post]
func (c *BackupController) CancelBackup(ctx *gin.Context) {
	user, ok := users_middleware.GetUserFromContext(ctx)
	if !ok {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "User not authenticated"})
		return
	}

	id, err := uuid.Parse(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup ID"})
		return
	}

	if err := c.backupService.CancelBackup(user, id); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.Status(http.StatusNoContent)
}

// GenerateDownloadToken
// @Summary Generate short-lived download token
// @Description Generate a token for downloading a backup file (valid for 5 minutes)
// @Tags backups
// @Param id path string true "Backup ID"
// @Success 200 {object} backups_download.GenerateDownloadTokenResponse
// @Failure 400
// @Failure 401
// @Failure 409 {object} map[string]string "Download already in progress"
// @Router /backups/{id}/download-token [post]
func (c *BackupController) GenerateDownloadToken(ctx *gin.Context) {
	user, ok := users_middleware.GetUserFromContext(ctx)
	if !ok {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "User not authenticated"})
		return
	}

	id, err := uuid.Parse(ctx.Param("id"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup ID"})
		return
	}

	response, err := c.backupService.GenerateDownloadToken(user, id)
	if err != nil {
		if err == backups_download.ErrDownloadAlreadyInProgress {
			ctx.JSON(
				http.StatusConflict,
				gin.H{
					"error": "Download already in progress for some of backups. Please wait until previous download completed or cancel it",
				},
			)
			return
		}
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, response)
}

// GetFile
// @Summary Download a backup file
// @Description Download the backup file for the specified backup using a download token.
// @Description
// @Description **Download Concurrency Control:**
// @Description - Only one download per user is allowed at a time
// @Description - If a download is already in progress, returns 409 Conflict
// @Description - Downloads are tracked using cache with 5-second TTL and 3-second heartbeat
// @Description - Browser cancellations automatically release the download lock
// @Description - Server crashes are handled via automatic cache expiry (5 seconds)
// @Tags backups
// @Param id path string true "Backup ID"
// @Param token query string true "Download token"
// @Success 200 {file} file
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 409 {object} map[string]string "Download already in progress"
// @Failure 500 {object} map[string]string
// @Router /backups/{id}/file [get]
func (c *BackupController) GetFile(ctx *gin.Context) {
	token := ctx.Query("token")
	if token == "" {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "download token is required"})
		return
	}

	backupIDParam := ctx.Param("id")
	backupID, err := uuid.Parse(backupIDParam)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid backup ID"})
		return
	}

	downloadToken, rateLimiter, err := c.backupService.ValidateDownloadToken(token)
	if err != nil {
		if err == backups_download.ErrDownloadAlreadyInProgress {
			ctx.JSON(
				http.StatusConflict,
				gin.H{
					"error": "download already in progress for this user. Please wait until previous download completed or cancel it",
				},
			)
			return
		}

		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired download token"})
		return
	}

	if downloadToken.BackupID != backupID {
		ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired download token"})
		return
	}

	fileReader, backup, database, err := c.backupService.GetBackupFileWithoutAuth(
		downloadToken.BackupID,
	)
	if err != nil {
		c.backupService.UnregisterDownload(downloadToken.UserID)
		c.backupService.ReleaseDownloadLock(downloadToken.UserID)
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rateLimitedReader := backups_download.NewRateLimitedReader(fileReader, rateLimiter)

	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	defer func() {
		cancelHeartbeat()
		c.backupService.UnregisterDownload(downloadToken.UserID)
		c.backupService.ReleaseDownloadLock(downloadToken.UserID)
		if err := rateLimitedReader.Close(); err != nil {
			fmt.Printf("Error closing file reader: %v\n", err)
		}
	}()

	go c.startDownloadHeartbeat(heartbeatCtx, downloadToken.UserID)

	filename := c.generateBackupFilename(backup, database)

	if backup.BackupSizeMb > 0 {
		sizeBytes := int64(backup.BackupSizeMb * 1024 * 1024)
		ctx.Header("Content-Length", fmt.Sprintf("%d", sizeBytes))
	}

	ctx.Header("Content-Type", "application/octet-stream")
	ctx.Header(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=\"%s\"", filename),
	)

	_, err = io.Copy(ctx.Writer, rateLimitedReader)
	if err != nil {
		fmt.Printf("Error streaming file: %v\n", err)
		return
	}

	c.backupService.WriteAuditLogForDownload(downloadToken.UserID, backup, database)
}

type MakeBackupRequest struct {
	DatabaseID uuid.UUID               `json:"database_id" binding:"required"`
	BackupType backups_core.BackupType `json:"backup_type"`
}

func (c *BackupController) generateBackupFilename(
	backup *backups_core.Backup,
	database *databases.Database,
) string {
	// Format timestamp as YYYY-MM-DD_HH-mm-ss
	timestamp := backup.CreatedAt.Format("2006-01-02_15-04-05")

	// Sanitize database name for filename (replace spaces and special chars)
	safeName := sanitizeFilename(database.Name)

	// Determine extension based on database type
	extension := c.getBackupExtension(database.Type, backup.Type)

	return fmt.Sprintf("%s_backup_%s%s", safeName, timestamp, extension)
}

func (c *BackupController) getBackupExtension(
	dbType databases.DatabaseType,
	backupType backups_core.BackupType,
) string {
	switch dbType {
	case databases.DatabaseTypeMysql, databases.DatabaseTypeMariadb:
		return ".sql.zst"
	case databases.DatabaseTypePostgres:
		if backupType == backups_core.BackupTypePITR {
			return ".tar.gz"
		}
		return ".dump"
	case databases.DatabaseTypeMongodb:
		return ".archive"
	default:
		return ".backup"
	}
}

func sanitizeFilename(name string) string {
	// Replace characters that are invalid in filenames
	replacer := map[rune]rune{
		' ':  '_',
		'/':  '-',
		'\\': '-',
		':':  '-',
		'*':  '-',
		'?':  '-',
		'"':  '-',
		'<':  '-',
		'>':  '-',
		'|':  '-',
	}

	result := make([]rune, 0, len(name))
	for _, char := range name {
		if replacement, exists := replacer[char]; exists {
			result = append(result, replacement)
		} else {
			result = append(result, char)
		}
	}

	return string(result)
}

func (c *BackupController) startDownloadHeartbeat(ctx context.Context, userID uuid.UUID) {
	ticker := time.NewTicker(backups_download.GetDownloadHeartbeatInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.backupService.RefreshDownloadLock(userID)
		}
	}
}
