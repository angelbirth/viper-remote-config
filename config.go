package config

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
)

func init() {
	viper.SupportedRemoteProviders = append(viper.SupportedRemoteProviders, "mysql")
}

type MysqlRemoteConfigFactory struct {
	db *sql.DB
	KeyFieldName,
	ValueFieldName string
}

func (p MysqlRemoteConfigFactory) Get(rp viper.RemoteProvider) (io.Reader, error) {
	var err error
	if p.db == nil {
		p.db, err = sql.Open(rp.Provider(), rp.Endpoint())
		if err != nil {
			return nil, err
		}
	}
	rows, err := p.db.Query(fmt.Sprintf(`select %s,%s from %s`, p.KeyFieldName, p.ValueFieldName, rp.Path()))
	if err != nil {
		return nil, err
	}
	var configs = make(map[string]json.RawMessage)
	for rows.Next() {
		var key string
		var value []byte
		err = rows.Scan(&key, &value)
		if err != nil {
			return nil, err
		}
		configs[key] = value
	}
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "")
	err = encoder.Encode(configs)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (p MysqlRemoteConfigFactory) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	var err error
	if p.db == nil {
		p.db, err = sql.Open(rp.Provider(), rp.Endpoint())
		if err != nil {
			return nil, err
		}
	}
	rows, err := p.db.Query(fmt.Sprintf(`select %s,%s from %s`, p.KeyFieldName, p.ValueFieldName, rp.Path()))
	if err != nil {
		return nil, err
	}
	var configs = make(map[string]json.RawMessage)
	for rows.Next() {
		var key string
		var value []byte
		err = rows.Scan(&key, &value)
		if err != nil {
			return nil, err
		}
		configs[key] = value
	}
	reader, writer := io.Pipe()
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "")
	err = encoder.Encode(configs)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (p MysqlRemoteConfigFactory) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	panic("not implemented")
}

type MysqlRemoteProvider struct {
	DSN, TableName string
}

func (d MysqlRemoteProvider) Provider() string {
	return "mysql"
}

func (d MysqlRemoteProvider) Endpoint() string {
	return d.DSN
}

func (d MysqlRemoteProvider) Path() string {
	return d.TableName
}

func (d MysqlRemoteProvider) SecretKeyring() string {
	return ""
}
