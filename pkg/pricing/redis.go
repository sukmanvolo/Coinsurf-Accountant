package pricing

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"
)

const gigabyte = 1073741824.0
const redisGetTimeout = time.Second * 5

type Redis struct {
	rd         *redis.Client
	pricePerGB float64
	ignoreByte float64
}

func getPricePerGB(rd *redis.Client, key string) (float64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisGetTimeout)
	defer cancel()

	price, err := rd.Get(ctx, key).Result()
	if err != nil {
		return 0, errors.New("pricing key was not found")
	}

	return strconv.ParseFloat(price, 10)
}

func NewRedis(logger *logrus.Logger, closing <-chan os.Signal, rd *redis.Client, key string, updateCh string, ignoreBytes float64) (*Redis, error) {
	pricePerGB, err := getPricePerGB(rd, key)
	if err != nil {
		return nil, err
	}

	r := &Redis{rd: rd, pricePerGB: pricePerGB, ignoreByte: ignoreBytes}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-closing
		cancel()
	}()

	go func() {
		sub := rd.Subscribe(ctx, updateCh)
		defer sub.Unsubscribe(context.Background(), updateCh)
		defer sub.Close()

		logger.Debug("waiting for price to be updated")
		for {
			select {
			case <-sub.Channel():
				r.pricePerGB, err = getPricePerGB(rd, key)
				if err != nil {
					logger.WithField("key", key).Error("failed to get price per GB")
				}

				logger.WithField("price", r.pricePerGB).Info("updated pricing")
			}
		}
	}()

	return r, nil
}

func (r *Redis) Price(bytes int64) (price float64) {
	b := float64(bytes)

	if b <= r.ignoreByte {
		return price
	}

	return (r.pricePerGB * b) / gigabyte
}
