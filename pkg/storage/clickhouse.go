package storage

import (
	"context"
	"github.com/coinsurf-com/accountant/pkg"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"time"
)

type ClickHouse struct {
	ch *sqlx.DB
}

//insert usage transactions in ClickHouse
const chInsertTransactions = `INSERT INTO transactions(user_id, node_id, data, millicent, type, timestamp, date)
								  VALUES (?, ?, ?, ?, ?, ?, ?)`

func NewClickHouse(ch *sqlx.DB) *ClickHouse {
	return &ClickHouse{ch: ch}
}

func (d *ClickHouse) Name() string {
	return "clickhouse"
}

func (d *ClickHouse) Save(ctx context.Context, data []pkg.NewData, users []pkg.User) error {
	chTransactions := make([][]interface{}, 0)
	for i, d := range data {
		var user *pkg.User
		for _, u := range users {
			if u.UUID == d.UserUUID {
				user = &u

				d.APIKey = u.APIKey
				data[i].APIKey = u.APIKey
				break
			}
		}

		if user == nil {
			log.WithFields(log.Fields{
				"uuid":    d.UserUUID,
				"api_key": d.APIKey,
			}).Warnf("%s:user %s:auth_token not found", d.UserUUID, d.APIKey)
			continue
		}

		//FIXME replace user uuid with customer uuid
		columns := []interface{}{d.UserUUID, d.NodeUUID, d.Transferred, d.Earning, "debit", time.Now(), time.Now()}
		chTransactions = append(chTransactions, columns)
	}

	tx, err := d.ch.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "BeginTx")
	}

	stmt, err := tx.Prepare(chInsertTransactions)
	if err != nil {
		return errors.Wrap(err, "Prepare")
	}

	for i := 0; i < len(chTransactions); i++ {
		if _, err := stmt.Exec(
			chTransactions[i][0],
			chTransactions[i][1],
			chTransactions[i][2],
			chTransactions[i][3],
			chTransactions[i][4],
			chTransactions[i][5],
			chTransactions[i][6],
		); err != nil {
			log.WithField("transaction", chTransactions[i]).Error("stmt.Exec error " + err.Error())
			return errors.Wrap(err, "")
		}
	}

	return tx.Commit()
}
