package storage

import (
	"context"
	"github.com/coinsurf-com/accountant/pkg"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Redis struct {
	rd       *redis.Client
	ranOutCh string
}

func NewRedis(rd *redis.Client, ranOutCh string) *Redis {
	return &Redis{rd: rd, ranOutCh: ranOutCh}
}

func (d *Redis) Name() string {
	return "redis"
}

func (d *Redis) Save(ctx context.Context, data []pkg.NewData, users []pkg.User) error {
	redisUserKeys := make([]string, 0)
	redisDataLeftKeys := make(map[string]int64, 0)
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

		redisUserKeys = append(redisUserKeys, pkg.UserKey(user.APIKey))
		redisDataLeftKeys[d.APIKey] = d.Transferred
	}

	//Decrement usage counter in Redis
	for key, transferred := range redisDataLeftKeys {
		e, err := d.rd.Exists(ctx, pkg.DataKey(key)).Result()
		if err != nil {
			log.WithField("data_key", pkg.DataKey(key)).Error(err)
			continue
		}

		if e == 0 {
			continue
		}

		value, err := d.rd.DecrBy(ctx, pkg.DataKey(key), transferred).Result()
		if err != nil {
			log.WithField("data_key", pkg.DataKey(key)).Error(errors.Wrap(err, "decrement data key"))
			continue
		}

		if value <= 0 {
			_, err = d.rd.Publish(ctx, d.ranOutCh, key).Result()
			if err != nil {
				log.Error(errors.Wrap(err, "publish to channel"))
			}

			_, err = d.rd.Del(ctx, pkg.UserKey(key)).Result()
			if err != nil {
				log.WithFields(log.Fields{
					"user_key": pkg.UserKey(key),
					"data_key": pkg.DataKey(key),
				}).Error(errors.Wrap(err, "delete keys"))
			}
		}
	}

	return nil
}
