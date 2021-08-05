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
	db           *badger.DB
	currentBatch *badger.Txn
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

// Once tendermint core has reached consensus on a block it needs to be
// communicated to the application
// The ABCI interface for that is
// BeginBlock -> signals that a new block has been committed to and transactions are coming
// DeliverTx -> this is used to send each transaction in the block (sent per transaction)
// EndBlock -> signals that all transactions for that block has been sent
// CommitBlock -> Applies the transactions in the block to the state machine in order

// BeginBlock opens a new write batch on badger db
func (app *KVStoreApplication) BeginBlock(req abcitypes.RequestBeginBlock) abcitypes.ResponseBeginBlock {
	app.currentBatch = app.db.NewTransaction(true)
	return abcitypes.ResponseBeginBlock{}
}

// DeliverTx validates the transaction again but also
// applies the transaction to the state machine
// returns a code to indicate if the transaction is valid
// I am not sure what the tendermint core will do if the application says
// that a transaction is not valid as a response to DeliverTx
func (app *KVStoreApplication) DeliverTx(req abcitypes.RequestDeliverTx) abcitypes.ResponseDeliverTx {
	code := app.isValid(req.Tx)
	if code != 0 {
		return abcitypes.ResponseDeliverTx{Code: code}
	}

	parts := bytes.Split(req.Tx, []byte("="))
	key, value := parts[0], parts[1]

	// Add the key value pair to the current batch
	// NOTE: There is a possibility that the current batch
	// might get too big, how would this be handled
	// since we can't commit yet???
	err := app.currentBatch.Set(key, value)
	if err != nil {
		panic(err)
	}

	return abcitypes.ResponseDeliverTx{Code: VALID_TX}
}

// EndBlock doesn't really do anything for this application
func (app *KVStoreApplication) EndBlock(req abcitypes.RequestEndBlock) abcitypes.ResponseEndBlock {
	return abcitypes.ResponseEndBlock{}
}

// Commit persistence all the transactions for the current batch i.e current block
func (app *KVStoreApplication) Commit() abcitypes.ResponseCommit {
	app.currentBatch.Commit()
	// Not sure what the Data is supposed to return
	return abcitypes.ResponseCommit{Data: []byte{}}
}
