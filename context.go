package otgorm

import (
	"context"

	"gorm.io/gorm"
)

// WithContext sets the current context in the db instance for instrumentation.
func WithContext(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.Set(contextScopeKey, ctx)
}
