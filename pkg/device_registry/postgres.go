package device_registry

import (
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"sync"
	"time"
)

const createDeviceSQL = `INSERT INTO devices (os, version, ip, user_id, app_version, id, online, created, updated) 
						 VALUES($1, $2, $3, $4, $5, gen_random_uuid(), NOW(), NOW(), NOW())
						 ON CONFLICT(os, version, ip, user_id) 
						 DO UPDATE SET app_version = EXCLUDED.app_version, online = NOW()`
const createDeviceHeaderSQL = `INSERT INTO devices (os, version, ip, user_id, app_version, id, online, created, updated) VALUES`
const createDeviceFooterSQL = ` ON CONFLICT(os, version, ip, user_id) DO UPDATE SET app_version = EXCLUDED.app_version, online = NOW()`

type Postgres struct {
	mu  *sync.RWMutex
	ips map[string]struct{}
	ch  chan string
	pg  *sqlx.DB
}

//TODO test batch upsert
func NewPostgres(closing <-chan os.Signal, chSize int, duration time.Duration, pg *sqlx.DB, logger *logrus.Logger) *Postgres {
	pgCh := make(chan string, chSize)

	postgre := &Postgres{mu: &sync.RWMutex{}, ips: make(map[string]struct{}), pg: pg, ch: pgCh}

	//go func() {
	//	defer close(pgCh)
	//	ticker := time.NewTicker(duration)
	//	defer ticker.Stop()
	//
	//	buf := bytebufferpool.Get()
	//	defer bytebufferpool.Put(buf)
	//
	//	for {
	//		select {
	//		case <-closing:
	//			return
	//		case <-ticker.C:
	//			r := bytebufferpool.Get()
	//			r.WriteString(createDeviceHeaderSQL)
	//			r.WriteString(buf.String())
	//			r.WriteString(createDeviceFooterSQL)
	//			fmt.Println(r.String())
	//			//_, err := postgre.pg.Exec(r.String())
	//			//if err != nil {
	//			//	logger.WithField("query", r.String()).WithError(err).Error("failed to execute query")
	//			//}
	//			bytebufferpool.Put(r)
	//
	//			postgre.mu.Lock()
	//			postgre.ips = make(map[string]struct{})
	//			postgre.mu.Unlock()
	//		case q := <-postgre.ch:
	//			if len(createDeviceHeaderSQL) < buf.Len() {
	//				buf.WriteString(",")
	//			}
	//			buf.WriteString(q)
	//		}
	//	}
	//}()

	return postgre
}

func (p *Postgres) Register(nodeId string, ip net.IP, os, version, appVersion string) error {
	//ipStr := ip.String()
	//p.mu.Lock()
	//
	//if _, ok := p.ips[ipStr]; ok {
	//	//record already in queue, no need to update
	//	p.mu.Unlock()
	//	return nil
	//}
	//
	//p.ips[ipStr] = struct{}{}
	//p.mu.Unlock()
	//
	//query := fmt.Sprintf(`('%s', '%s', '%s', '%s', '%s', gen_random_uuid(), NOW(), NOW(), NOW())`,
	//	os, version, ipStr, nodeId, appVersion)
	//p.ch <- query

	_, err := p.pg.Exec(createDeviceSQL, os, version, ip.String(), nodeId, appVersion)
	return err
}
