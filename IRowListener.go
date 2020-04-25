package dbq

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

type IRowListener interface {
	Consume(rowId int64) int
}
