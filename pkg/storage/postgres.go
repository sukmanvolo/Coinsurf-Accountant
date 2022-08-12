package storage

import (
	"context"
	"fmt"
	"github.com/coinsurf-com/accountant/pkg"
	"github.com/jmoiron/sqlx"
	"strings"
)

type Postgres struct {
	pg *sqlx.DB
}

func NewPostgres(pg *sqlx.DB) *Postgres {
	return &Postgres{pg: pg}
}

func (d *Postgres) Name() string {
	return "postgres"
}

func (d *Postgres) Save(ctx context.Context, data []pkg.NewData, _ []pkg.User) error {
	var transactions = make([]string, len(data))

	for i, r := range data {
		transaction := fmt.Sprintf("('%s', '%s', %d, %.4f)", r.NodeUUID, r.UserUUID, r.Transferred, r.Earning)
		transactions[i] = transaction
	}

	const pgUpdateDataLeft = `BEGIN;
				DROP TABLE IF EXISTS temp_updates;
                CREATE TEMP TABLE temp_updates(
						id serial primary key, 
						node_uuid uuid not null, 
						user_uuid uuid not null, 
						transferred bigint, 
						earnings decimal);
                INSERT INTO temp_updates(node_uuid, user_uuid, transferred, earnings) VALUES %s;	
				
                UPDATE users t
                SET 
					data = data - u.transferred, 
					earnings_current = earnings_current + u.earnings, 
					earnings_total = earnings_total + u.earnings, 
					data_updated = NOW()
                FROM temp_updates u
                WHERE t.id = u.user_uuid;
				
				UPDATE users t
                SET 
					data = data + u.transferred, 
					earnings_current = earnings_current + u.earnings, 
					earnings_total = earnings_total + u.earnings, 
					data_updated = NOW()
                FROM temp_updates u
                WHERE t.id = u.node_uuid;
                COMMIT`
	query := fmt.Sprintf(pgUpdateDataLeft, strings.Join(transactions, ","))
	_, err := d.pg.ExecContext(ctx, query)
	return err
}
