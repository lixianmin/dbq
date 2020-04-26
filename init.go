package dbq

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

var logger ILogger = &ConsoleLogger{}

func init() {

}

func Init(log ILogger) {
	if log != nil {
		logger = log
	}
}
