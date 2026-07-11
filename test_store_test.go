package blockqueue

import (
	"github.com/jmoiron/sqlx"
	"github.com/yudhasubki/blockqueue/store"
)

func testDB(driver store.Driver) *sqlx.DB {
	return sqlx.NewDb(driver.DB(), driver.DriverName())
}
