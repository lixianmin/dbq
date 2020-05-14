package dbi

import (
	"context"
	"database/sql"
)

/********************************************************************
created:    2020-05-13
author:     lixianmin

Copyright (C) - All Rights Reserved
 *********************************************************************/

const (
	BeginTx = iota
	QueryContext
	ExecContext
	TxQueryContext
	TxExecContext
	TxCommit
	TxRollback
)

var emptyHandler = func(ctx *Context) {}

type Context struct {
	context.Context
	Kind      int
	Text      string
	//StartTime time.Time
	err       error
}

func (ctx *Context) Err() error {
	if ctx.err != nil {
		return ctx.err
	}

	return ctx.Context.Err()
}

type DB struct {
	DB                 *sql.DB
	preExecuteHandler  func(*Context)
	postExecuteHandler func(*Context)
}

func NewDB(db *sql.DB) *DB {
	var my = &DB{
		DB:                 db,
		preExecuteHandler:  emptyHandler,
		postExecuteHandler: emptyHandler,
	}

	return my
}

func newContext(ctx context.Context, kind int, text string) *Context {
	return &Context{
		Context:   ctx,
		Kind:      kind,
		Text:      text,
		//StartTime: time.Now(),
	}
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	var ctx1 = newContext(ctx, BeginTx, "BeginTx")

	db.preExecuteHandler(ctx1)
	var tx, err = db.DB.BeginTx(ctx1, opts)
	ctx1.err = err
	db.postExecuteHandler(ctx1)

	var tx1 = &Tx{TX: tx, db: db}
	return tx1, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var ctx1 = newContext(ctx, QueryContext, query)

	db.preExecuteHandler(ctx1)
	var rows, err = db.DB.QueryContext(ctx1, query, args...)
	ctx1.err = err
	db.postExecuteHandler(ctx1)
	return rows, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var ctx1 = newContext(ctx, ExecContext, query)

	db.postExecuteHandler(ctx1)
	var result, err = db.DB.ExecContext(ctx1, query, args...)
	ctx1.err = err
	db.postExecuteHandler(ctx1)
	return result, err
}

func (db *DB) SetPreExecuteHandler(handler func(ctx *Context)) {
	if handler != nil {
		db.preExecuteHandler = handler
	} else {
		db.preExecuteHandler = emptyHandler
	}
}

func (db *DB) SetPostExecuteHandler(handler func(ctx *Context)) {
	if handler != nil {
		db.postExecuteHandler = handler
	} else {
		db.postExecuteHandler = emptyHandler
	}
}

////////////////////////////////////////////////////////////////////////
type Tx struct {
	TX *sql.Tx
	db *DB
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var ctx1 = newContext(ctx, TxQueryContext, query)

	tx.db.preExecuteHandler(ctx1)
	var rows, err = tx.TX.QueryContext(ctx1, query, args...)
	ctx1.err = err
	tx.db.postExecuteHandler(ctx1)
	return rows, err
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var ctx1 = newContext(ctx, TxExecContext, query)

	tx.db.preExecuteHandler(ctx1)
	var result, err = tx.TX.ExecContext(ctx1, query, args...)
	ctx1.err = err
	tx.db.postExecuteHandler(ctx1)
	return result, err
}

func (tx *Tx) Commit() error {
	var ctx1 = newContext(context.Background(), TxCommit, "TxCommit")

	tx.db.preExecuteHandler(ctx1)
	var err = tx.TX.Commit()
	ctx1.err = err
	tx.db.postExecuteHandler(ctx1)
	return err
}

func (tx *Tx) Rollback() error {
	var ctx1 = newContext(context.Background(), TxRollback, "TxRollback")

	tx.db.preExecuteHandler(ctx1)
	var err = tx.TX.Rollback()
	ctx1.err = err
	tx.db.postExecuteHandler(ctx1)
	return err
}
