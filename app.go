package main

import (
	"bytes"
	"github.com/dgraph-io/badger"
	abcitypes "github.com/tendermint/tendermint/abci/types"
)

// Tendermint core, handles network (peer communication) and consensus between peers
// Application defines the state machine
// The goal is to make sure the current state of the state machine is the same across
// all correct nodes

const VALID_TX uint32 = 0

type KVStoreApplication struct {
	db *badger.DB
}

var _ abcitypes.Application = (*KVStoreApplication)(nil)

func NewKVStoreApplication(db *badger.DB) *KVStoreApplication {
	return &KVStoreApplication{
		db: db,
	}
}


// When a peer gets a transaction from another peer, it has to confirm with
// the application to determine if the transaction is valid
// if valid it adds it to the mempool, else it discards it

// CheckTx weakly validates the transaction
// i.e. validates the transaction without applying it to the state machine
func (app *KVStoreApplication) CheckTx(req abcitypes.RequestCheckTx) abcitypes.ResponseCheckTx {
	code := app.isValid(req.Tx)
	return abcitypes.ResponseCheckTx{Code: code, GasWanted: 1}
}

// isValid validates that a transaction meets a set of constraints
// in the case of this application, the constraint will be that
// the transaction must follow the format 'key=value'
// and that the exact key=value pair must not already exist
// as nothing new is being added to the database
func (app *KVStoreApplication) isValid(tx []byte) (code uint32) {

	// if the code value is a non-zero value then the transaction
	// is considered invalid by tendermint core
	// I technically don't need to put this here as the default value
	// for uint32 is 0, but this feels much clearer
	code = VALID_TX

	// check transaction format is of type 'key=value'
	parts := bytes.Split(tx, []byte("="))
	if len(parts) != 2 {
		return 1 // Invalidates the transaction
	}

	key, value := parts[0], parts[1]

	// check if the sane key=value pair already exist
	err := app.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		// The only permitted error is that the key was not found
		// if we get any other error, return that so we can panic
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		// We enter this branch if the key was found, now we need
		// to verify that the value is not the same
		if err == nil {
			return item.Value(func(val []byte) error {
				if bytes.Equal(val, value) {
					code = 2 // Invalidates the transaction
				}
				return nil
			})
		}
		// Db checks done, no database error encountered
		return nil
	})

	if err != nil {
		// err can only not be nil if something went wrong with the db
		panic(err)
	}

	return code
}
