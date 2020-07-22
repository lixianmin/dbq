package dbq

import (
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
