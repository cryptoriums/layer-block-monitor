package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	TableNameReports = "reports"
	TableNameTxs     = "txs"
	TableNameRewards = "rewards"
)

// SQLDB wraps *sql.DB to satisfy the Db interface with context helpers.
type SQLDB struct{ *sql.DB }

// Db defines the required database methods.
type Db interface {
	Exec(context.Context, string, ...any) (sql.Result, error)
	Query(context.Context, string, ...any) (*sql.Rows, error)
	Prepare(context.Context, string) (*sql.Stmt, error)
}

// New wraps the provided *sql.DB and ensures the monitor tables exist.
func New(ctx context.Context, raw *sql.DB) (SQLDB, error) {
	wrapped := SQLDB{DB: raw}
	if err := EnsureTables(ctx, wrapped); err != nil {
		return SQLDB{}, err
	}
	return wrapped, nil
}

// EnsureTables creates the required tables if they do not exist.
func EnsureTables(ctx context.Context, database Db) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := initReportsTable(ctx, database); err != nil {
		return err
	}
	if err := initTxTable(ctx, database); err != nil {
		return err
	}
	return initRewardsTable(ctx, database)
}

func (s SQLDB) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return s.DB.ExecContext(ctx, q, args...)
}

func (s SQLDB) Query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.DB.QueryContext(ctx, q, args...)
}

func (s SQLDB) Prepare(ctx context.Context, q string) (*sql.Stmt, error) {
	return s.DB.PrepareContext(ctx, q)
}

func initTxTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			block_height UInt64,
			tx_hash      FixedString(64),
			sender       String,
			gas_used     UInt64,
			fee_amount   Decimal(30, 6)
		)
		ENGINE = MergeTree
		PARTITION BY intDiv(block_height, 100000)
		ORDER BY (block_height, tx_hash)
	`, TableNameTxs)

	_, err := database.Exec(ctx, query)
	return err
}

func initRewardsTable(ctx context.Context, database Db) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			block_height UInt64,
			reporter     String,
			query_id     String,
			amount       Decimal(30, 6)
		)
		ENGINE = MergeTree
		PARTITION BY intDiv(block_height, 100000)
		ORDER BY (block_height, reporter)
	`, TableNameRewards)

	_, err := database.Exec(ctx, query)
	return err
}

func initReportsTable(ctx context.Context, database Db) error {
	columnDefs := []string{
		fmt.Sprintf("%s LowCardinality(String)", reporterCol),
		fmt.Sprintf("%s UInt64", powerCol),
		fmt.Sprintf("%s LowCardinality(String)", queryTypeCol),
		fmt.Sprintf("%s String", queryIdCol),
		fmt.Sprintf("%s LowCardinality(String)", aggregateMethodCol),
		fmt.Sprintf("%s String", valueCol),
		fmt.Sprintf("%s DateTime64(3, 'UTC')", timestampCol),
		fmt.Sprintf("%s UInt8", cyclelistCol),
		fmt.Sprintf("%s UInt64", blockNumberCol),
		fmt.Sprintf("%s UInt64", metaIdCol),
	}

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
		%[2]s
		)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(%[3]s)
		ORDER BY (%[4]s, %[5]s)
		SETTINGS index_granularity = 8192
		`,
		TableNameReports,
		strings.Join(columnDefs, ",\n  "),
		timestampCol,
		blockNumberCol,
		queryIdCol,
	)

	_, err := database.Exec(ctx, query)
	return err
}

const (
	reporterCol        = "reporter"
	powerCol           = "power"
	queryTypeCol       = "query_type"
	queryIdCol         = "query_id"
	aggregateMethodCol = "aggregate_method"
	valueCol           = "value"
	timestampCol       = "timestamp"
	cyclelistCol       = "cyclelist"
	blockNumberCol     = "block_number"
	metaIdCol          = "meta_id"
)
