package dbq

import (
	"database/sql"
	"github.com/lixianmin/dbq/logger"
	"github.com/lixianmin/logo"
)

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

func Init(log logo.ILogger) {
	logger.Init(log)
}

func dot(err error) error {
	if err != nil && err != sql.ErrTxDone && err != sql.ErrNoRows {
		logger.GetLogger().Error("err=%q", err)
	}
	return err
}
