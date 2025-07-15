package transaction

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TransactionManager manages database transactions
type TransactionManager struct {
	transactions map[uint64]*types.Transaction
	nextTxnID    uint64
	lockManager  *LockManager
	walManager   *WALManager
	mu           sync.RWMutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(dataDir string) (*TransactionManager, error) {
	walManager, err := NewWALManager(dataDir + "/wal.log")
	if err != nil {
		return nil, err
	}

	return &TransactionManager{
		transactions: make(map[uint64]*types.Transaction),
		nextTxnID:    1,
		lockManager:  NewLockManager(),
		walManager:   walManager,
	}, nil
}

// Begin starts a new transaction
func (tm *TransactionManager) Begin() *types.Transaction {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txnID := atomic.AddUint64(&tm.nextTxnID, 1)
	txn := &types.Transaction{
		ID:        txnID,
		State:     types.TxnActive,
		StartTime: time.Now(),
		ReadSet:   make(map[types.TupleID]bool),
		WriteSet:  make(map[types.TupleID]*types.Tuple),
		LockSet:   make(map[types.TupleID]types.LockType),
		Isolation: types.ReadCommitted,
	}

	tm.transactions[txnID] = txn

	// Write begin log record
	logRecord := &types.LogRecord{
		TxnID:     txnID,
		Type:      types.LogBegin,
		Timestamp: time.Now(),
	}
	tm.walManager.WriteLog(logRecord)

	return txn
}

// Commit commits a transaction
func (tm *TransactionManager) Commit(txn *types.Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if txn.State != types.TxnActive {
		return fmt.Errorf("transaction %d is not active", txn.ID)
	}

	// Write commit log record
	logRecord := &types.LogRecord{
		TxnID:     txn.ID,
		Type:      types.LogCommit,
		Timestamp: time.Now(),
	}
	tm.walManager.WriteLog(logRecord)

	// Flush WAL
	tm.walManager.FlushLog()

	// Release all locks
	tm.lockManager.ReleaseAllLocks(txn.ID)

	// Update transaction state
	txn.State = types.TxnCommitted

	// Remove from active transactions
	delete(tm.transactions, txn.ID)

	return nil
}

// Rollback rolls back a transaction
func (tm *TransactionManager) Rollback(txn *types.Transaction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if txn.State != types.TxnActive {
		return fmt.Errorf("transaction %d is not active", txn.ID)
	}

	// Write abort log record
	logRecord := &types.LogRecord{
		TxnID:     txn.ID,
		Type:      types.LogAbort,
		Timestamp: time.Now(),
	}
	tm.walManager.WriteLog(logRecord)

	// Release all locks
	tm.lockManager.ReleaseAllLocks(txn.ID)

	// Update transaction state
	txn.State = types.TxnAborted

	// Remove from active transactions
	delete(tm.transactions, txn.ID)

	return nil
}

// GetTransaction retrieves a transaction by ID
func (tm *TransactionManager) GetTransaction(txnID uint64) (*types.Transaction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	return txn, nil
}

// LockManager manages locks for transactions
type LockManager struct {
	locks map[types.TupleID]*Lock
	queue map[types.TupleID][]*LockRequest
	mu    sync.RWMutex
}

// Lock represents a lock on a tuple
type Lock struct {
	TID     types.TupleID
	Type    types.LockType
	Owners  []uint64 // Transaction IDs
	Waiters []uint64
}

// LockRequest represents a lock request
type LockRequest struct {
	TxnID    uint64
	Type     types.LockType
	Granted  bool
	Response chan error
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[types.TupleID]*Lock),
		queue: make(map[types.TupleID][]*LockRequest),
	}
}

// AcquireLock acquires a lock on a tuple
func (lm *LockManager) AcquireLock(txnID uint64, tid types.TupleID, lockType types.LockType) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if lock can be granted immediately
	if lm.canGrantLock(tid, lockType, txnID) {
		lm.grantLock(tid, lockType, txnID)
		return nil
	}

	// Add to wait queue
	request := &LockRequest{
		TxnID:    txnID,
		Type:     lockType,
		Granted:  false,
		Response: make(chan error, 1),
	}

	lm.queue[tid] = append(lm.queue[tid], request)

	// Release mutex and wait for response
	lm.mu.Unlock()
	err := <-request.Response
	lm.mu.Lock()

	return err
}

// canGrantLock checks if a lock can be granted
func (lm *LockManager) canGrantLock(tid types.TupleID, lockType types.LockType, txnID uint64) bool {
	lock, exists := lm.locks[tid]
	if !exists {
		return true
	}

	switch lockType {
	case types.SharedLock:
		return lock.Type == types.SharedLock
	case types.ExclusiveLock:
		return len(lock.Owners) == 1 && lock.Owners[0] == txnID
	}

	return false
}

// grantLock grants a lock to a transaction
func (lm *LockManager) grantLock(tid types.TupleID, lockType types.LockType, txnID uint64) {
	lock, exists := lm.locks[tid]
	if !exists {
		lock = &Lock{
			TID:     tid,
			Type:    lockType,
			Owners:  []uint64{txnID},
			Waiters: []uint64{},
		}
		lm.locks[tid] = lock
	} else {
		lock.Owners = append(lock.Owners, txnID)
	}
}

// ReleaseLock releases a lock held by a transaction
func (lm *LockManager) ReleaseLock(txnID uint64, tid types.TupleID) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lock, exists := lm.locks[tid]
	if !exists {
		return fmt.Errorf("no lock found for tuple %v", tid)
	}

	// Remove transaction from owners
	for i, owner := range lock.Owners {
		if owner == txnID {
			lock.Owners = append(lock.Owners[:i], lock.Owners[i+1:]...)
			break
		}
	}

	// If no more owners, remove lock
	if len(lock.Owners) == 0 {
		delete(lm.locks, tid)
		// Process waiting requests
		lm.processWaitingRequests(tid)
	}

	return nil
}

// ReleaseAllLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseAllLocks(txnID uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Find all locks owned by this transaction
	var tidsToRelease []types.TupleID
	for tid, lock := range lm.locks {
		for _, owner := range lock.Owners {
			if owner == txnID {
				tidsToRelease = append(tidsToRelease, tid)
				break
			}
		}
	}

	// Release all locks
	for _, tid := range tidsToRelease {
		lm.ReleaseLock(txnID, tid)
	}

	return nil
}

// processWaitingRequests processes requests waiting for a lock
func (lm *LockManager) processWaitingRequests(tid types.TupleID) {
	requests, exists := lm.queue[tid]
	if !exists || len(requests) == 0 {
		return
	}

	// Grant lock to first compatible request
	for i, request := range requests {
		if lm.canGrantLock(tid, request.Type, request.TxnID) {
			lm.grantLock(tid, request.Type, request.TxnID)
			request.Response <- nil
			// Remove from queue
			lm.queue[tid] = append(requests[:i], requests[i+1:]...)
			return
		}
	}
}

// WALManager manages write-ahead logging
type WALManager struct {
	logFile    *os.File
	buffer     []byte
	currentLSN uint64
	mu         sync.Mutex
}

// NewWALManager creates a new WAL manager
func NewWALManager(filename string) (*WALManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WALManager{
		logFile:    file,
		buffer:     make([]byte, 0, 4096),
		currentLSN: 1,
	}, nil
}

// WriteLog writes a log record to the WAL
func (wal *WALManager) WriteLog(record *types.LogRecord) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	record.LSN = atomic.AddUint64(&wal.currentLSN, 1)

	// Serialize log record (simplified)
	data := fmt.Sprintf("LSN:%d TxnID:%d Type:%d Timestamp:%s\n",
		record.LSN, record.TxnID, record.Type, record.Timestamp.Format(time.RFC3339))

	// Write to buffer
	wal.buffer = append(wal.buffer, []byte(data)...)

	// Flush if buffer is full
	if len(wal.buffer) >= 4096 {
		return wal.FlushLog()
	}

	return nil
}

// FlushLog flushes the WAL buffer to disk
func (wal *WALManager) FlushLog() error {
	if len(wal.buffer) == 0 {
		return nil
	}

	_, err := wal.logFile.Write(wal.buffer)
	if err != nil {
		return err
	}

	err = wal.logFile.Sync()
	if err != nil {
		return err
	}

	wal.buffer = wal.buffer[:0]
	return nil
}

// Recover performs crash recovery using WAL
func (wal *WALManager) Recover() error {
	// Simplified recovery - just ensure file exists
	_, err := wal.logFile.Stat()
	return err
}

// Close closes the WAL manager
func (wal *WALManager) Close() error {
	wal.FlushLog()
	return wal.logFile.Close()
}