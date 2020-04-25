package dbq

/********************************************************************
created:    2020-04-25
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

type IRowListener interface {
	consume(rowId int64) int
}
