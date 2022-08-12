package server

import (
	"context"
	"errors"
	"github.com/coinsurf-com/accountant/pkg"
	"github.com/coinsurf-com/accountant/pkg/device_registry"
	"github.com/coinsurf-com/accountant/pkg/pricing"
	"github.com/coinsurf-com/accountant/pkg/referral"
	"github.com/coinsurf-com/accountant/pkg/storage"
	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/mailru/easyjson"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

type Config struct {
	Debug          bool `long:"debug" env:"DEBUG"`
	PrometheusPort int  `long:"prometheus" env:"PROMETHEUS_PORT" default:"3000" description:""`

	Postgres                          string        `long:"postgres" env:"POSTGRES" default:""`
	PostgresDeviceRegistryChannelSize int           `long:"postgres-dr-channel-size" env:"POSTGRES_DEVICE_REGISTRY_CHANNEL_SIZE" default:"500"`
	PostgresDeviceRegistryFlush       time.Duration `long:"postgres-dr-flush-period" env:"POSTGRES_DEVICE_REGISTRY_FLUSH_PERIOD" default:"15s"`

	ClickHouse string `long:"clickhouse" env:"CLICKHOUSE" default:""`
	Redis      string `long:"redis" env:"REDIS" default:""`

	RedisChannelUserRemove string `long:"redis-ch-user-remove" env:"REDIS_CH_USER_REMOVE" default:"user.remove"`
	RedisChannelUserUsage  string `long:"redis-ch-user-usage" env:"REDIS_CH_USER_USAGE" default:"user.usage"`
	RedisChannelUserOnline string `long:"redis-ch-user-online" env:"REDIS_CH_USER_ONLINE" default:"user.online"`

	RedisChannelPricingUpdate string `long:"redis-ch-pricing-update" env:"REDIS_CH_PRICING_UPDATE" default:"pricing"`
	RedisChannelPayoutUpdate  string `long:"redis-ch-payout-update" env:"REDIS_CH_PAYOUT_UPDATE" default:"payout"`

	RedisPricingKey string `long:"redis-pricing-key" env:"REDIS_PRICING_KEY" default:":4:pricing"`
	RedisPayoutKey  string `long:"redis-payout-key" env:"REDIS_PAYOUT_KEY" default:":4:payout"`

	MinBytesAmount      float64 `long:"min-bytes-amount" env:"MIN_BYTES_AMOUNT" default:"1024.0" description:"data less than value will be ignored"`
	GhostsCheckInterval int     `long:"ghosts-check-interval" env:"GHOSTS_CHECK_INTERVAL" default:"900" description:"seconds"`
}

func Listen(closing <-chan os.Signal, config *Config, logger *log.Logger) error {
	ch := newClickHouse(config.ClickHouse)
	logger.Debugf("ClickHouse: connected to %s", config.ClickHouse)

	pg := newPostgres(config.Postgres)
	logger.Debugf("Postgres: connected to %s", config.Postgres)

	rd := newRedis(config.Redis)
	logger.Debugf("Redis: connected to %s", config.Redis)
	defer func() {
		ch.Close()
		pg.Close()
		rd.Close()
	}()

	ghostsCheck := time.NewTicker(time.Second * time.Duration(config.GhostsCheckInterval))
	defer ghostsCheck.Stop()

	accountants := []pkg.Storage{
		storage.NewPostgres(pg),
		storage.NewRedis(rd, config.RedisChannelUserRemove),
		storage.NewClickHouse(ch),
	}

	p, err := pricing.NewRedis(logger, closing, rd, config.RedisPricingKey, config.RedisChannelPricingUpdate, config.MinBytesAmount)
	if err != nil {
		return err
	}

	defaultAccountant := pkg.NewDefault(logger,
		closing, ghostsCheck,
		pg,
		p,
		accountants...,
	)

	deviceRegistry := device_registry.NewPostgres(closing, config.PostgresDeviceRegistryChannelSize, config.PostgresDeviceRegistryFlush, pg, logger)
	referrals, err := referral.NewPostgres(logger, closing, rd, config.RedisPayoutKey, config.RedisChannelPayoutUpdate, pg)
	if err != nil {
		return err
	}

	onlineSub := rd.Subscribe(context.Background(), config.RedisChannelUserOnline)
	defer onlineSub.Unsubscribe(context.Background(), config.RedisChannelUserOnline)
	defer onlineSub.Close()

	go listenOnline(closing, onlineSub.Channel(), deviceRegistry, referrals, logger)

	usageSub := rd.Subscribe(context.Background(), config.RedisChannelUserUsage)
	defer usageSub.Unsubscribe(context.Background(), config.RedisChannelUserUsage)
	defer usageSub.Close()

	//pub sub data usage sync
	go listenUsage(closing, usageSub.Channel(), defaultAccountant, logger)

	<-closing

	return nil
}

func listenOnline(closing <-chan os.Signal, ch <-chan *redis.Message, registry pkg.DeviceRegistry, referrals pkg.Referral, logger *log.Logger) error {
	refTicker := time.NewTimer(time.Hour)
	defer refTicker.Stop()

	for {
		for {
			select {
			case msg := <-ch:
				parts := strings.Split(msg.Payload, ",")
				var nodeId = parts[0]
				var ip = net.ParseIP(parts[1])
				var nodeOs = parts[2]
				var versionOs = parts[3]
				var appVersion = parts[4]

				err := registry.Register(nodeId, ip, nodeOs, versionOs, appVersion)
				if err != nil {
					logger.WithFields(map[string]interface{}{
						"node_id":     nodeId,
						"ip":          parts[1],
						"os":          nodeOs,
						"version":     versionOs,
						"app_version": appVersion,
					}).WithError(err).Error("failed to register node")
				}

				err = referrals.Register(nodeId, ip)
				if err != nil {
					logger.WithFields(map[string]interface{}{
						"node_id": nodeId,
						"ip":      parts[1],
					}).WithError(err).Error("failed to register referral")
				}
			case <-refTicker.C:
				err := referrals.Payout()
				if err != nil {
					logger.WithError(err).Error("failed to call a referral payout")
				}
			case <-closing:
				return errors.New("received kill")
			}
		}
	}
}

func listenUsage(closing <-chan os.Signal, ch <-chan *redis.Message, accountant *pkg.Base, logger *log.Logger) error {
	for {
		select {
		case msg := <-ch:
			var cache pkg.Results
			err := easyjson.Unmarshal(s2b(msg.Payload), &cache)
			if err != nil {
				logger.WithError(err).WithField("payload", msg).Error("failed to unmarshal payload")
				continue
			}

			for nodeId, usage := range cache {
				err = accountant.Record(nodeId, usage.Users)
				if err != nil {
					logger.
						WithField("node_id", nodeId).
						WithField("usage", usage).
						WithError(err).
						Error("failed to record usage")
				}
			}
		case <-closing:
			return errors.New("close received")
		}
	}
}

func newRedis(dsn string) *redis.Client {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	options, err := redis.ParseURL(dsn)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(options)

	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		panic(err)
	}

	return rdb
}

func newClickHouse(dsn string) *sqlx.DB {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	db, err := sqlx.ConnectContext(ctx, "clickhouse", dsn)
	if err != nil {
		panic(err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		panic(err)
	}

	return db
}

func newPostgres(dsn string) *sqlx.DB {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	return db
}

func s2b(s string) (b []byte) {
	/* #nosec G103 */
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	/* #nosec G103 */
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}
