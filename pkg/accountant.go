package pkg

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Base struct {
	mu              sync.RWMutex
	ghosts          map[string]struct{}
	pg              *sqlx.DB
	UpdateTimeout   time.Duration
	StopSaveOnError bool
	pricing         Pricing
	logger          *logrus.Logger

	storages []Storage
}

//note. I'm aware of possible SQL injection here, but since user input is not expected here I don't mind
func NewDefault(logger *logrus.Logger, closing <-chan os.Signal, ghostTicker *time.Ticker, pg *sqlx.DB, pricing Pricing, storages ...Storage) *Base {
	base := &Base{
		mu:              sync.RWMutex{},
		ghosts:          make(map[string]struct{}),
		pg:              pg,
		pricing:         pricing,
		storages:        storages,
		UpdateTimeout:   time.Second * 5,
		StopSaveOnError: false,
		logger:          logger,
	}

	go func() {
		for {
			select {
			case <-ghostTicker.C:
				rows, err := pg.Query(`SELECT id FROM users WHERE is_ghost = true`)
				if err != nil {
					logger.WithError(err).Error("failed to query ghost users")
					continue
				}

				var ghosts = 1
				base.mu.Lock()
				for rows.Next() {
					var id string
					err = rows.Scan(&id)
					if err != nil {
						logger.WithError(err).Error("failed to scan ghost user id to string")
						continue
					}

					base.ghosts[id] = struct{}{}
					ghosts++
				}
				base.mu.Unlock()

				err = rows.Close()
				if err != nil {
					logger.WithError(err).Error("failed to close ghost users rows")
				}

				logger.Info("updated ghosts " + strconv.Itoa(ghosts))
			case <-closing:
				return
			}
		}
	}()

	return base
}

func (d *Base) Record(nodeId string, usage map[string]int64) error {
	var userUuids = make([]string, 0)
	var data = make([]NewData, 0)
	var i = 0
	var total int64

	d.mu.RLock()
	for uid, transferred := range usage {
		if _, ok := d.ghosts[uid]; ok {
			//don't record usage of the ghost users
			continue
		}

		userUuids = append(userUuids, fmt.Sprintf("'%s'", uid))
		earnings := d.pricing.Price(transferred)
		data = append(data, NewData{Earning: earnings, Transferred: transferred, UserUUID: uid, NodeUUID: nodeId})
		total += transferred
		i++
	}
	d.mu.RUnlock()

	d.logger.Infof("users: %d, transferred %d kb", len(userUuids), total/1000)

	if len(data) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), d.UpdateTimeout)
	defer cancel()

	users, err := d.getUsersByUUID(ctx, userUuids...)
	if err != nil {
		return errors.Wrap(err, "getUsersByUUID")
	}

	for _, storage := range d.storages {
		ctx, cancel = context.WithTimeout(context.Background(), d.UpdateTimeout)
		err = storage.Save(ctx, data, users)
		cancel()
		if err != nil {
			if d.StopSaveOnError {
				return err
			}

			d.logger.WithFields(map[string]interface{}{
				"data":    data,
				"storage": storage.Name(),
			}).Error("failed to save data to storage, err: " + err.Error())
		}
	}

	return nil
}

func (d *Base) getUsersByUUID(ctx context.Context, userUuids ...string) ([]User, error) {
	const pgUsersDataLeft = `SELECT id, data, proxy_password
							 FROM users
							 WHERE id IN (%s)`

	query := fmt.Sprintf(pgUsersDataLeft, strings.Join(userUuids, ","))
	rows, err := d.pg.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}

	var data = make([]User, 0)
	for rows.Next() {
		var user User
		err = rows.Scan(&user.UUID, &user.Data, &user.APIKey)
		if err != nil {
			return nil, err
		}

		data = append(data, user)
	}

	return data, nil
}
