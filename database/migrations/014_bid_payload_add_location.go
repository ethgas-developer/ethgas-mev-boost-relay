package migrations

import (
	"bitbucket.org/infinity-exchange/mev-boost-relay/database/vars"
	migrate "github.com/rubenv/sql-migrate"
)

var Migration014AddLocation = &migrate.Migration{
	Id: "014-add-location",
	Up: []string{`
		ALTER TABLE ` + vars.TableBuilderBlockSubmission + ` ADD locate varchar(50);
		ALTER TABLE ` + vars.TableGetPayload + ` ADD locate varchar(50);
		ALTER TABLE ` + vars.TableTooLateGetPayload + ` ADD locate varchar(50);
		ALTER TABLE ` + vars.TableDeliveredPayload + ` ADD locate varchar(50);
	`},
	Down: []string{},

	DisableTransactionUp:   true,
	DisableTransactionDown: true,
}
