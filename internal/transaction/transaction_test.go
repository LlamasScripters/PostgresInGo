package transaction

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TestNewTransactionManager tests transaction manager creation
func TestNewTransactionManager(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	if tm == nil {
		t.Fatal("Transaction manager should not be nil")
	}

	if tm.transactions == nil {
		t.Error("Transactions map should not be nil")
	}

	if tm.nextTxnID != 1 {
		t.Errorf("Expected next transaction ID to be 1, got %d", tm.nextTxnID)
	}

	if tm.lockManager == nil {
		t.Error("Lock manager should not be nil")
	}

	if tm.walManager == nil {
		t.Error("WAL manager should not be nil")
	}

	if len(tm.transactions) != 0 {
		t.Errorf("Expected 0 transactions, got %d", len(tm.transactions))
	}
}

// TestTransactionManagerBegin tests transaction creation
func TestTransactionManagerBegin(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin first transaction
	txn1 := tm.Begin()
	if txn1 == nil {
		t.Fatal("Transaction should not be nil")
	}

	if txn1.ID == 0 {
		t.Error("Transaction ID should not be 0")
	}

	if txn1.State != types.TxnActive {
		t.Errorf("Expected transaction state to be TxnActive, got %v", txn1.State)
	}

	if txn1.StartTime.IsZero() {
		t.Error("Transaction start time should not be zero")
	}

	if txn1.ReadSet == nil {
		t.Error("Read set should not be nil")
	}

	if txn1.WriteSet == nil {
		t.Error("Write set should not be nil")
	}

	if txn1.LockSet == nil {
		t.Error("Lock set should not be nil")
	}

	if txn1.Isolation != types.ReadCommitted {
		t.Errorf("Expected isolation level ReadCommitted, got %v", txn1.Isolation)
	}

	// Begin second transaction
	txn2 := tm.Begin()
	if txn2 == nil {
		t.Fatal("Second transaction should not be nil")
	}

	if txn2.ID == txn1.ID {
		t.Error("Transaction IDs should be different")
	}

	if txn2.ID <= txn1.ID {
		t.Error("Second transaction ID should be greater than first")
	}

	// Verify both transactions are tracked
	if len(tm.transactions) != 2 {
		t.Errorf("Expected 2 active transactions, got %d", len(tm.transactions))
	}
}

// TestTransactionManagerCommit tests transaction commit
func TestTransactionManagerCommit(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin transaction
	txn := tm.Begin()
	if txn == nil {
		t.Fatal("Transaction should not be nil")
	}

	// Commit transaction
	err = tm.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify transaction state
	if txn.State != types.TxnCommitted {
		t.Errorf("Expected transaction state to be TxnCommitted, got %v", txn.State)
	}

	// Verify transaction is removed from active transactions
	if len(tm.transactions) != 0 {
		t.Errorf("Expected 0 active transactions after commit, got %d", len(tm.transactions))
	}

	// Test committing already committed transaction
	err = tm.Commit(txn)
	if err == nil {
		t.Error("Should fail when committing already committed transaction")
	}
}

// TestTransactionManagerRollback tests transaction rollback
func TestTransactionManagerRollback(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin transaction
	txn := tm.Begin()
	if txn == nil {
		t.Fatal("Transaction should not be nil")
	}

	// Rollback transaction
	err = tm.Rollback(txn)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify transaction state
	if txn.State != types.TxnAborted {
		t.Errorf("Expected transaction state to be TxnAborted, got %v", txn.State)
	}

	// Verify transaction is removed from active transactions
	if len(tm.transactions) != 0 {
		t.Errorf("Expected 0 active transactions after rollback, got %d", len(tm.transactions))
	}

	// Test rolling back already aborted transaction
	err = tm.Rollback(txn)
	if err == nil {
		t.Error("Should fail when rolling back already aborted transaction")
	}
}

// TestTransactionManagerConcurrency tests concurrent transactions
func TestTransactionManagerConcurrency(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Test concurrent transaction creation
	numTxns := 10
	done := make(chan *types.Transaction, numTxns)

	for i := 0; i < numTxns; i++ {
		go func() {
			txn := tm.Begin()
			done <- txn
		}()
	}

	// Collect all transactions
	var txns []*types.Transaction
	for i := 0; i < numTxns; i++ {
		txn := <-done
		txns = append(txns, txn)
	}

	// Verify all transactions have unique IDs
	seen := make(map[uint64]bool)
	for _, txn := range txns {
		if seen[txn.ID] {
			t.Errorf("Duplicate transaction ID: %d", txn.ID)
		}
		seen[txn.ID] = true
	}

	// Test concurrent commits
	commitDone := make(chan error, numTxns)
	for _, txn := range txns {
		go func(t *types.Transaction) {
			err := tm.Commit(t)
			commitDone <- err
		}(txn)
	}

	// Wait for all commits
	for i := 0; i < numTxns; i++ {
		err := <-commitDone
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
		}
	}

	// Verify all transactions are committed
	if len(tm.transactions) != 0 {
		t.Errorf("Expected 0 active transactions after all commits, got %d", len(tm.transactions))
	}
}

// TestTransactionManagerLockIntegration tests lock integration
func TestTransactionManagerLockIntegration(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin transaction
	txn := tm.Begin()
	if txn == nil {
		t.Fatal("Transaction should not be nil")
	}

	// Test lock acquisition
	tupleID := types.TupleID{PageID: 1, Offset: 0}
	err = tm.lockManager.AcquireLock(txn.ID, tupleID, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Commit transaction (should release locks)
	err = tm.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test that lock was released by trying to acquire exclusive lock
	err = tm.lockManager.AcquireLock(txn.ID+1, tupleID, types.ExclusiveLock)
	if err != nil {
		t.Error("Should be able to acquire exclusive lock after commit released shared lock")
	}
}

// TestTransactionManagerIsolationLevels tests isolation levels
func TestTransactionManagerIsolationLevels(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin transaction with default isolation
	txn := tm.Begin()
	if txn.Isolation != types.ReadCommitted {
		t.Errorf("Expected default isolation ReadCommitted, got %v", txn.Isolation)
	}

	// Test setting different isolation levels
	txn.Isolation = types.ReadUncommitted
	if txn.Isolation != types.ReadUncommitted {
		t.Errorf("Expected ReadUncommitted, got %v", txn.Isolation)
	}

	txn.Isolation = types.RepeatableRead
	if txn.Isolation != types.RepeatableRead {
		t.Errorf("Expected RepeatableRead, got %v", txn.Isolation)
	}

	txn.Isolation = types.Serializable
	if txn.Isolation != types.Serializable {
		t.Errorf("Expected Serializable, got %v", txn.Isolation)
	}

	// Commit transaction
	err = tm.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestTransactionManagerReadWriteSets tests read/write set tracking
func TestTransactionManagerReadWriteSets(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin transaction
	txn := tm.Begin()
	if txn == nil {
		t.Fatal("Transaction should not be nil")
	}

	// Test read set tracking
	tupleID1 := types.TupleID{PageID: 1, Offset: 0}
	txn.ReadSet[tupleID1] = true

	if !txn.ReadSet[tupleID1] {
		t.Error("Tuple should be in read set")
	}

	// Test write set tracking
	tupleID2 := types.TupleID{PageID: 1, Offset: 1}
	tuple := &types.Tuple{
		TID:  tupleID2,
		Data: []byte("test data"),
	}
	txn.WriteSet[tupleID2] = tuple

	if txn.WriteSet[tupleID2] != tuple {
		t.Error("Tuple should be in write set")
	}

	// Test lock set tracking
	tupleID3 := types.TupleID{PageID: 1, Offset: 2}
	txn.LockSet[tupleID3] = types.ExclusiveLock

	if txn.LockSet[tupleID3] != types.ExclusiveLock {
		t.Error("Lock should be in lock set")
	}

	// Commit transaction
	err = tm.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

// TestNewLockManager tests lock manager creation
func TestNewLockManager(t *testing.T) {
	lm := NewLockManager()
	if lm == nil {
		t.Fatal("Lock manager should not be nil")
	}

	if lm.locks == nil {
		t.Error("Locks map should not be nil")
	}

	if lm.queue == nil {
		t.Error("Queue map should not be nil")
	}

	if len(lm.locks) != 0 {
		t.Errorf("Expected 0 locks, got %d", len(lm.locks))
	}
}

// TestLockManagerAcquireLock tests lock acquisition
func TestLockManagerAcquireLock(t *testing.T) {
	lm := NewLockManager()
	tupleID := types.TupleID{PageID: 1, Offset: 0}
	txnID := uint64(1)

	// Test acquiring shared lock
	err := lm.AcquireLock(txnID, tupleID, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}

	// Test acquiring another shared lock on same tuple
	err = lm.AcquireLock(txnID+1, tupleID, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire second shared lock: %v", err)
	}

	// Test acquiring exclusive lock (should fail)
	err = lm.AcquireLock(txnID+2, tupleID, types.ExclusiveLock)
	if err == nil {
		t.Error("Should fail to acquire exclusive lock when shared locks exist")
	}

	// Release all locks
	lm.ReleaseAllLocks(txnID)
	lm.ReleaseAllLocks(txnID + 1)

	// Now exclusive lock should succeed
	err = lm.AcquireLock(txnID+2, tupleID, types.ExclusiveLock)
	if err != nil {
		t.Fatalf("Failed to acquire exclusive lock: %v", err)
	}

	// Verify exclusive lock exists by checking lock map
	if _, exists := lm.locks[tupleID]; !exists {
		t.Error("Exclusive lock should exist in lock map")
	}
}

// TestLockManagerReleaseLock tests lock release
func TestLockManagerReleaseLock(t *testing.T) {
	lm := NewLockManager()
	tupleID := types.TupleID{PageID: 1, Offset: 0}
	txnID := uint64(1)

	// Acquire lock
	err := lm.AcquireLock(txnID, tupleID, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Release lock
	err = lm.ReleaseLock(txnID, tupleID)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}

	// Verify lock is released by checking lock map
	if _, exists := lm.locks[tupleID]; exists {
		t.Error("Lock should be released and removed from map")
	}

	// Test releasing non-existent lock
	err = lm.ReleaseLock(txnID, tupleID)
	if err == nil {
		t.Error("Should fail when releasing non-existent lock")
	}
}

// TestLockManagerReleaseAllLocks tests releasing all locks for a transaction
func TestLockManagerReleaseAllLocks(t *testing.T) {
	lm := NewLockManager()
	txnID := uint64(1)

	// Acquire multiple locks
	tupleID1 := types.TupleID{PageID: 1, Offset: 0}
	tupleID2 := types.TupleID{PageID: 1, Offset: 1}
	tupleID3 := types.TupleID{PageID: 2, Offset: 0}

	err := lm.AcquireLock(txnID, tupleID1, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire lock 1: %v", err)
	}

	err = lm.AcquireLock(txnID, tupleID2, types.SharedLock)
	if err != nil {
		t.Fatalf("Failed to acquire lock 2: %v", err)
	}

	err = lm.AcquireLock(txnID, tupleID3, types.ExclusiveLock)
	if err != nil {
		t.Fatalf("Failed to acquire lock 3: %v", err)
	}

	// Verify all locks are held by checking lock map
	if _, exists := lm.locks[tupleID1]; !exists {
		t.Error("Lock 1 should be held")
	}
	if _, exists := lm.locks[tupleID2]; !exists {
		t.Error("Lock 2 should be held")
	}
	if _, exists := lm.locks[tupleID3]; !exists {
		t.Error("Lock 3 should be held")
	}

	// Release all locks
	lm.ReleaseAllLocks(txnID)

	// Verify all locks are released by checking lock map
	if _, exists := lm.locks[tupleID1]; exists {
		t.Error("Lock 1 should be released")
	}
	if _, exists := lm.locks[tupleID2]; exists {
		t.Error("Lock 2 should be released")
	}
	if _, exists := lm.locks[tupleID3]; exists {
		t.Error("Lock 3 should be released")
	}
}

// TestNewWALManager tests WAL manager creation
func TestNewWALManager(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("wal_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	walFile := filepath.Join(testDir, "wal.log")
	wm, err := NewWALManager(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}

	if wm == nil {
		t.Fatal("WAL manager should not be nil")
	}

	if wm.logFile == nil {
		t.Error("WAL log file should not be nil")
	}

	// Test file creation
	if _, err := os.Stat(walFile); os.IsNotExist(err) {
		t.Error("WAL file should have been created")
	}

	// Clean up
	err = wm.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL manager: %v", err)
	}
}

// TestWALManagerWriteLog tests log writing
func TestWALManagerWriteLog(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("wal_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	walFile := filepath.Join(testDir, "wal.log")
	wm, err := NewWALManager(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Write log record
	logRecord := &types.LogRecord{
		TxnID:     1,
		Type:      types.LogBegin,
		Timestamp: time.Now(),
		Data:      []byte("test log data"),
	}

	err = wm.WriteLog(logRecord)
	if err != nil {
		t.Fatalf("Failed to write log record: %v", err)
	}

	// Verify log record was written by checking currentLSN
	if wm.currentLSN == 0 {
		t.Error("Current LSN should be incremented after writing log")
	}

	// Write another log record
	logRecord2 := &types.LogRecord{
		TxnID:     1,
		Type:      types.LogCommit,
		Timestamp: time.Now(),
		Data:      []byte("commit log data"),
	}

	err = wm.WriteLog(logRecord2)
	if err != nil {
		t.Fatalf("Failed to write second log record: %v", err)
	}

	// Verify LSN incremented
	if wm.currentLSN <= 1 {
		t.Error("Current LSN should be incremented after writing second log")
	}
}

// TestWALManagerFlushLog tests log flushing
func TestWALManagerFlushLog(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("wal_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	walFile := filepath.Join(testDir, "wal.log")
	wm, err := NewWALManager(walFile)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	// Write log record
	logRecord := &types.LogRecord{
		TxnID:     1,
		Type:      types.LogBegin,
		Timestamp: time.Now(),
		Data:      []byte("test log data"),
	}

	err = wm.WriteLog(logRecord)
	if err != nil {
		t.Fatalf("Failed to write log record: %v", err)
	}

	// Flush log
	err = wm.FlushLog()
	if err != nil {
		t.Fatalf("Failed to flush log: %v", err)
	}

	// Verify file has data
	fileInfo, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}

	if fileInfo.Size() == 0 {
		t.Error("WAL file should have data after flush")
	}
}

// TestTransactionManagerErrorHandling tests error handling
func TestTransactionManagerErrorHandling(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Test committing nil transaction
	err = tm.Commit(nil)
	if err == nil {
		t.Error("Should fail when committing nil transaction")
	}

	// Test rolling back nil transaction
	err = tm.Rollback(nil)
	if err == nil {
		t.Error("Should fail when rolling back nil transaction")
	}

	// Test with invalid transaction state
	fakeTxn := &types.Transaction{
		ID:    999,
		State: types.TxnCommitted,
	}

	err = tm.Commit(fakeTxn)
	if err == nil {
		t.Error("Should fail when committing already committed transaction")
	}

	err = tm.Rollback(fakeTxn)
	if err == nil {
		t.Error("Should fail when rolling back already committed transaction")
	}
}

// TestTransactionManagerCleanup tests proper cleanup
func TestTransactionManagerCleanup(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("txn_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	tm, err := NewTransactionManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}

	// Begin multiple transactions
	txns := make([]*types.Transaction, 5)
	for i := 0; i < 5; i++ {
		txns[i] = tm.Begin()
	}

	// Commit some, rollback others
	for i, txn := range txns {
		if i%2 == 0 {
			err = tm.Commit(txn)
			if err != nil {
				t.Fatalf("Failed to commit transaction %d: %v", i, err)
			}
		} else {
			err = tm.Rollback(txn)
			if err != nil {
				t.Fatalf("Failed to rollback transaction %d: %v", i, err)
			}
		}
	}

	// Verify all transactions are cleaned up
	if len(tm.transactions) != 0 {
		t.Errorf("Expected 0 active transactions after cleanup, got %d", len(tm.transactions))
	}

	// Verify transaction IDs are still incrementing
	newTxn := tm.Begin()
	if newTxn.ID <= 5 {
		t.Error("Transaction ID should continue incrementing after cleanup")
	}

	err = tm.Commit(newTxn)
	if err != nil {
		t.Fatalf("Failed to commit new transaction: %v", err)
	}
}