package daos

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/pavan-intelops/test1/device_service/pkg/rest/server/daos/clients/sqls"
	"github.com/pavan-intelops/test1/device_service/pkg/rest/server/models"
	log "github.com/sirupsen/logrus"
)

type DeviceDao struct {
	sqlClient *sqls.MySQLClient
}

func migrateDevices(r *sqls.MySQLClient) error {
	query := `
	CREATE TABLE IF NOT EXISTS devices(
		ID int NOT NULL AUTO_INCREMENT,
        
		Name VARCHAR(100) NOT NULL,
	    PRIMARY KEY (ID)
	);
	`
	_, err := r.DB.Exec(query)
	return err
}

func NewDeviceDao() (*DeviceDao, error) {
	sqlClient, err := sqls.InitMySQLDB()
	if err != nil {
		return nil, err
	}
	err = migrateDevices(sqlClient)
	if err != nil {
		return nil, err
	}
	return &DeviceDao{
		sqlClient,
	}, nil
}

func (deviceDao *DeviceDao) CreateDevice(m *models.Device) (*models.Device, error) {
	insertQuery := "INSERT INTO devices(Name) values(?)"
	res, err := deviceDao.sqlClient.DB.Exec(insertQuery, m.Name)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) {
			if mysqlErr.Number == 1062 {
				return nil, sqls.ErrDuplicate
			}
		}
		return nil, err
	}

	id, err := res.LastInsertId()
	if err != nil {
		return nil, err
	}
	m.Id = id
	log.Debugf("device created")
	return m, nil
}

func (deviceDao *DeviceDao) UpdateDevice(id int64, m *models.Device) (*models.Device, error) {
	if id == 0 {
		return nil, errors.New("invalid device ID")
	}
	if id != m.Id {
		return nil, errors.New("id and payload don't match")
	}

	device, err := deviceDao.GetDevice(id)
	if err != nil {
		return nil, err
	}
	if device == nil {
		return nil, sql.ErrNoRows
	}

	updateQuery := "UPDATE devices SET Name = ? WHERE Id = ?"
	res, err := deviceDao.sqlClient.DB.Exec(updateQuery, m.Name, id)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, sqls.ErrUpdateFailed
	}

	log.Debugf("device updated")
	return m, nil
}

func (deviceDao *DeviceDao) DeleteDevice(id int64) error {
	deleteQuery := "DELETE FROM devices WHERE Id = ?"
	res, err := deviceDao.sqlClient.DB.Exec(deleteQuery, id)
	if err != nil {
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return sqls.ErrDeleteFailed
	}

	log.Debugf("device deleted")
	return nil
}

func (deviceDao *DeviceDao) ListDevices() ([]*models.Device, error) {
	selectQuery := "SELECT * FROM devices"
	rows, err := deviceDao.sqlClient.DB.Query(selectQuery)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	var devices []*models.Device
	for rows.Next() {
		m := models.Device{}
		if err = rows.Scan(&m.Id, &m.Name); err != nil {
			return nil, err
		}
		devices = append(devices, &m)
	}
	if devices == nil {
		devices = []*models.Device{}
	}
	log.Debugf("device listed")
	return devices, nil
}

func (deviceDao *DeviceDao) GetDevice(id int64) (*models.Device, error) {
	selectQuery := "SELECT * FROM devices WHERE Id = ?"
	row := deviceDao.sqlClient.DB.QueryRow(selectQuery, id)

	m := models.Device{}
	if err := row.Scan(&m.Id, &m.Name); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sqls.ErrNotExists
		}
		return nil, err
	}
	log.Debugf("device retrieved")
	return &m, nil
}
