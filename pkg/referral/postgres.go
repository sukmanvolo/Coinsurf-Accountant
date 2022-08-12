package referral

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	updateReferralSQL = `UPDATE referrals 
					     SET referral_id = $1, status = 'registered' 
					     WHERE user_id != referral_id AND ip = $2 AND status = 'pending'`
)

const redisGetTimeout = time.Second * 5

type Postgres struct {
	minDataForPayout int64
	pg               *sqlx.DB
}

func (p *Postgres) Payout() error {
	//TODO finish
	//p.pg.Query(`SELECT u.id
	//				  FROM referrals r
	//				  LEFT JOIN users u ON r.user_id = u.id
	//				  WHERE r.status = 'registered' AND r.payed IS NULL AND u.data >= $1`)
	return nil
}

func getMinPayoutData(rd *redis.Client, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisGetTimeout)
	defer cancel()

	price, err := rd.Get(ctx, key).Result()
	if err != nil {
		return 0, errors.New("payout key was not found")
	}

	return strconv.ParseInt(price, 10, 64)
}

func NewPostgres(logger *logrus.Logger, closing <-chan os.Signal, rd *redis.Client, key string, updateCh string, pg *sqlx.DB) (*Postgres, error) {
	p := &Postgres{pg: pg}

	var err error
	p.minDataForPayout, err = getMinPayoutData(rd, key)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-closing
		cancel()
	}()

	go func() {
		sub := rd.Subscribe(ctx, updateCh)
		defer sub.Unsubscribe(context.Background(), updateCh)
		defer sub.Close()

		for {
			select {
			case <-sub.Channel():
				p.minDataForPayout, err = getMinPayoutData(rd, key)
				if err != nil {
					logger.WithField("key", key).Error("failed to get min payout data")
				}

				logger.WithField("price", p.minDataForPayout).Info("updated min payout data")
			}
		}
	}()

	return p, nil
}

func (p *Postgres) Register(nodeId string, ip net.IP) error {
	_, err := p.pg.Exec(updateReferralSQL, nodeId, ip.String())
	return err
}
