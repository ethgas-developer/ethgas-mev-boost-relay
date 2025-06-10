package migrations

import (
	"bitbucket.org/infinity-exchange/mev-boost-relay/database/vars"
	migrate "github.com/rubenv/sql-migrate"
)

var Migration016CreateConstraint = &migrate.Migration{
	Id: "016-create-constraint",
	Up: []string{`
		ALTER TABLE ` + vars.TableGetPayload + `
		ADD CONSTRAINT ` + vars.TableGetPayload + `_slot_pk_hash_locate_unique
		UNIQUE (slot, proposer_pubkey, block_hash, locate);

		ALTER TABLE ` + vars.TableTooLateGetPayload + `
		ADD CONSTRAINT ` + vars.TableTooLateGetPayload + `_slot_pk_hash_locate_unique
		UNIQUE (slot, proposer_pubkey, block_hash, locate);


	`},
	Down: []string{},

	DisableTransactionUp:   true,
	DisableTransactionDown: true,
}
