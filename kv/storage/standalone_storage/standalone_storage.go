package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db: engine_util.CreateDB("kv", false),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := new(StandAloneStorageReader)
	reader.txn = s.db.NewTransaction(false)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// writeBatch := new(engine_util.WriteBatch)
	// for _, element := range batch {
	// 	switch element.Data.(type) {
	// 	case storage.Put:
	// 		writeBatch.SetCF(element.Cf(), element.Key(), element.Value())
	// 	case storage.Delete:
	// 		writeBatch.DeleteCF(element.Cf(), element.Key())
	// 	}
	// }
	// err := writeBatch.WriteToDB(s.db)

	txn := s.db.NewTransaction(true)
	for _, w := range batch {
		switch w.Data.(type) {
		case storage.Put:
			put := w.Data.(storage.Put)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := w.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				return err
			}
		}
	}
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *StandAloneStorageReader) Close() {
	s.txn.Discard()
}
