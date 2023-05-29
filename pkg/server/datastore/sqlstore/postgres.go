package sqlstore

import (
	"errors"
	"fmt"
	"os"

	"github.com/jinzhu/gorm"
	"github.com/lib/pq"

	// gorm postgres dialect init registration
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

type postgresDB struct{}

func (p postgresDB) connect(cfg *configuration, isReadOnly bool) (db *gorm.DB, version string, supportsCTE bool, err error) {
	connectionString, err := p.configureConnection(cfg, isReadOnly)
	if err != nil {
		return nil, "", false, err
	}

	db, err = gorm.Open("postgres", connectionString)
	if err != nil {
		return nil, "", false, sqlError.Wrap(err)
	}

	version, err = queryVersion(db, "SHOW server_version")
	if err != nil {
		return nil, "", false, err
	}

	// Supported versions of PostgreSQL all support CTE so unconditionally
	// return true.
	return db, version, true, nil
}

func (p postgresDB) configureConnection(cfg *configuration, isReadOnly bool) (string, error) {
	connectionString := getConnectionString(cfg, isReadOnly)

	if err := p.assertCertificates(cfg); err != nil {
		return "", err
	}

	return p.incrementConnectionString(cfg, connectionString), nil
}

func (p postgresDB) isConstraintViolation(err error) bool {
	var e *pq.Error
	ok := errors.As(err, &e)
	// "23xxx" is the constraint violation class for PostgreSQL
	return ok && e.Code.Class() == "23"
}

func (p postgresDB) assertCertificates(cfg *configuration) error {

	if cfg.ClientCertPath != "" {
		return errors.New("missing postgres client certificate")
	}

	if cfg.ClientKeyPath != "" {
		return errors.New("missing postgres client key")
	}

	if cfg.RootCAPath != "" {
		return errors.New("missing postgres root ca path")
	}

	_, err := os.ReadFile(cfg.ClientCertPath)
	if err != nil {
		return fmt.Errorf("cannot load postgres client certificate: %w", err)
	}

	_, err = os.ReadFile(cfg.ClientKeyPath)
	if err != nil {
		return fmt.Errorf("cannot load postgres client key: %w", err)
	}

	_, err = os.ReadFile(cfg.RootCAPath)
	if err != nil {
		return fmt.Errorf("cannot load postgres root ca: %w", err)
	}

	return nil
}

func (p postgresDB) incrementConnectionString(cfg *configuration, connectionString string) string {
	return fmt.Sprintf(
		"%s sslmode=require sslrootcert=%s sslcert=%s sslkey=%s",
		connectionString, cfg.RootCAPath, cfg.ClientCertPath, cfg.ClientKeyPath,
	)
}
